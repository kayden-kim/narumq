package rollout

import (
	"errors"
	"fmt"
	"strings"

	rolloutapp "narumq/internal/application/rollout"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type ControllerConfig struct {
	Namespace       string
	ProducerID      string
	ProducerVersion string
	Registries      []string
}

func RunController(config ControllerConfig, audit rolloutapp.AuditPublisher) error {
	if config.Namespace == "" || config.ProducerID == "" || config.ProducerVersion == "" || len(config.Registries) == 0 || audit == nil {
		return errors.New("rollout controller runtime configuration is incomplete")
	}
	registries := make([]string, len(config.Registries))
	for index, registry := range config.Registries {
		registries[index] = strings.TrimSpace(registry)
		if registries[index] == "" {
			return errors.New("rollout controller registry allowlist contains an empty entry")
		}
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kubernetes APIs: %w", err)
	}
	if err := AddToScheme(scheme); err != nil {
		return fmt.Errorf("register rollout API: %w", err)
	}
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cache.Options{DefaultNamespaces: map[string]cache.Config{config.Namespace: {}}},
		LeaderElection:                true,
		LeaderElectionID:              "narumq-rollout-controller.narumq.io",
		LeaderElectionNamespace:       config.Namespace,
		LeaderElectionReleaseOnCancel: true,
		HealthProbeBindAddress:        ":8081",
		Metrics:                       server.Options{BindAddress: "0"},
	})
	if err != nil {
		return fmt.Errorf("create rollout controller manager: %w", err)
	}
	kubernetes, err := NewKubernetesRuntime(manager.GetClient(), manager.GetAPIReader())
	if err != nil {
		return err
	}
	images, err := NewOCIImageMetadataReader(manager.GetAPIReader(), registries)
	if err != nil {
		return fmt.Errorf("configure OCI metadata reader: %w", err)
	}
	service, err := rolloutapp.NewService(
		rolloutapp.ServiceConfig{ProducerVersion: config.ProducerVersion, ProducerInstanceID: config.ProducerID},
		kubernetes, kubernetes, images, NewManagementClient(), NewPodReadinessProbe(), audit, SystemClock{},
	)
	if err != nil {
		return fmt.Errorf("configure rollout application: %w", err)
	}
	reconciler, err := NewReconciler(manager.GetClient(), manager.GetAPIReader(), service)
	if err != nil {
		return err
	}
	if err := reconciler.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register rollout reconciler: %w", err)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("register liveness probe: %w", err)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("register readiness probe: %w", err)
	}
	return manager.Start(ctrl.SetupSignalHandler())
}
