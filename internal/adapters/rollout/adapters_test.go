package rollout

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	rolloutapp "narumq/internal/application/rollout"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStatusConversionRoundtripAndBounds(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 34, 56, 0, time.UTC)
	metaNow := metav1.NewTime(now)
	safe := true
	status := rolloutapp.StatusDTO{
		Phase: "Draining", ObservedGeneration: 4, AttemptKey: "attempt-1", RetryTokenHash: "hash-1",
		CurrentOrdinal: 1, CurrentPodName: "broker-1", CurrentPodUID: "uid-1", CurrentPodRV: "rv-1",
		DrainOperationID: "op-1", DrainIntentAt: &now, DrainStartedAt: &now, DrainDeadlineAt: &now,
		DrainPhase: "TRANSFERRING_SESSIONS", DrainState: "DRAINING", DrainSafeToStop: &safe,
		SafeToStopObservedAt: &now, DrainConnectionsInitial: 10, DrainConnectionsRemaining: 2,
		DrainSessionsReady: 5, DrainSessionsTakenOver: 3, DrainSessionsFailed: 0,
		DrainPendingWAL: 0, DrainUnderReplicated: 0, DrainFailureCode: "", DrainBlockingReason: "",
		DeleteIntent: true, FailureCode: "", FailureReason: "", CompletedOrdinals: []int32{0},
		TargetImage: "registry.test/narumq@sha256:" + strings.Repeat("a", 64), TargetVersion: "1.2.0",
		TargetFeatures: []string{"compatibility-probe"}, WorkloadUID: "wl-1", WorkloadName: "broker",
		RegistrySecretName: "reg-secret", ManagementEndpoint: "http://narumq-management.narumq.svc:8080",
		ManagementSecretName: "mgmt-secret", WorkloadGeneration: 4, LastTransitionTime: now,
		Conditions: []rolloutapp.ConditionDTO{{Type: "Ready", Status: "True", Reason: "Good", Message: "fine", ObservedGeneration: 4, LastTransitionTime: now}},
	}
	resource, err := statusToResource(status)
	if err != nil {
		t.Fatalf("statusToResource: %v", err)
	}
	if resource.CurrentOrdinal != 1 || resource.WorkloadName != "broker" || resource.ManagementEndpoint != status.ManagementEndpoint ||
		resource.DrainConnectionsRemaining != 2 || *resource.DrainSafeToStop != true {
		t.Fatalf("resource status fields unexpected: %+v", resource)
	}
	back, err := statusFromResource(resource)
	if err != nil {
		t.Fatalf("statusFromResource: %v", err)
	}
	if back.CurrentOrdinal != status.CurrentOrdinal || back.WorkloadName != status.WorkloadName ||
		back.DrainConnectionsRemaining != status.DrainConnectionsRemaining || !back.LastTransitionTime.Equal(status.LastTransitionTime) {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", back, status)
	}

	invalid := resource
	invalid.DrainConnectionsInitial = -1
	if _, err := statusFromResource(invalid); err == nil {
		t.Fatal("negative counter was accepted")
	}

	_ = metaNow
}

func TestManagementClientSafeURLValidationAndHeaders(t *testing.T) {
	var capturedAuth, capturedKey, capturedMethod, capturedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		capturedKey = r.Header.Get("X-API-Key")
		capturedMethod = r.Method
		capturedPath = r.URL.Path
		switch r.URL.Path {
		case "/api/v1/cluster/features":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"activeFeatures": []string{"compatibility-probe"},
					"members": []map[string]any{
						{"nodeId": "1", "releaseVersion": "1.1.0", "features": []string{"compatibility-probe"}},
						{"nodeId": "2", "releaseVersion": "1.1.0", "features": []string{"compatibility-probe"}},
						{"nodeId": "3", "releaseVersion": "1.1.0", "features": []string{"compatibility-probe"}},
					},
				},
			})
		case "/api/v1/nodes/1/drain":
			if r.Method == http.MethodGet {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": map[string]any{
						"operationId": "op-1", "nodeId": "1", "state": "DRAINING", "phase": "TRANSFERRING_SESSIONS",
						"startedAt": "2026-10-14T12:00:00Z", "deadlineAt": "2026-10-14T12:10:00Z",
						"connections": map[string]any{"initial": 10, "remaining": 0},
						"sessions":    map[string]any{"stateReadyForTakeover": 5, "takenOver": 5, "failed": 0},
						"replication": map[string]any{"pendingWal": 0, "underReplicated": 0},
						"safeToStop":  true,
					},
				})
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	if _, err := managementURL("http://evil.com:8080", "narumq"); err == nil {
		t.Fatal("external host was accepted")
	}
	if _, err := managementURL("https://narumq-management.narumq.svc:8080", "narumq"); err == nil {
		t.Fatal("HTTPS scheme was accepted")
	}
	if _, err := managementURL("http://narumq-management.other.svc:8080", "narumq"); err == nil {
		t.Fatal("cross-namespace management URL was accepted")
	}

	client := NewManagementClient()
	client.client = server.Client()

	originalURL, _ := url.Parse(server.URL)
	port := originalURL.Port()
	fakeEndpoint := fmt.Sprintf("http://narumq-management.narumq.svc:%s", port)

	customTransport := server.Client().Transport.(*http.Transport).Clone()
	client.client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req.URL.Scheme = originalURL.Scheme
		req.URL.Host = originalURL.Host
		return customTransport.RoundTrip(req)
	})

	features, err := client.ClusterFeatures(context.Background(), "narumq", "http://narumq-management.narumq.svc:8080", "operator-secret-key")
	if err != nil {
		t.Fatalf("ClusterFeatures: %v", err)
	}
	if len(features.Members) != 3 || !slices.Equal(features.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("features = %+v", features)
	}
	if capturedKey != "operator-secret-key" || capturedAuth != "" || capturedMethod != http.MethodGet || capturedPath != "/api/v1/cluster/features" {
		t.Fatalf("auth/request mismatch: key=%q auth=%q method=%q path=%q", capturedKey, capturedAuth, capturedMethod, capturedPath)
	}

	obs, found, err := client.GetDrain(context.Background(), "narumq", "http://narumq-management.narumq.svc:8080", "operator-secret-key", 1, "key-1")
	if err != nil || !found || obs.OperationID != "op-1" || obs.NodeID != 1 || obs.SafeToStop == nil || !*obs.SafeToStop {
		t.Fatalf("GetDrain = %+v found=%v err=%v", obs, found, err)
	}

	_ = fakeEndpoint
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestPodReadinessProbeDirect(t *testing.T) {
	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"UP"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer readyServer.Close()

	probe := NewPodReadinessProbe()
	probe.client = readyServer.Client()

	originalURL, _ := url.Parse(readyServer.URL)
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	probe.client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req.URL.Scheme = originalURL.Scheme
		req.URL.Host = originalURL.Host
		return baseTransport.RoundTrip(req)
	})

	if err := probe.Ready(context.Background(), "127.0.0.1"); err != nil {
		t.Fatalf("expected ready: %v", err)
	}
	if err := probe.Ready(context.Background(), "not-an-ip"); err == nil {
		t.Fatal("invalid IP was accepted")
	}
}

func TestOCIImageMetadataReaderAllowsOnlyConfiguredRegistryAndValidatesSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "narumq", Name: "narumq-ecr"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{
				corev1.DockerConfigJsonKey: []byte(`{"auths":{"809446814769.dkr.ecr.ap-northeast-2.amazonaws.com":{"auth":"dXNlcjpwYXNz"}}}`),
			},
		},
	).Build()

	reader, err := NewOCIImageMetadataReader(fakeClient, []string{"https://809446814769.dkr.ecr.ap-northeast-2.amazonaws.com"})
	if err != nil {
		t.Fatalf("NewOCIImageMetadataReader: %v", err)
	}

	// 1. Rejects mutable tags
	if _, err := reader.ReadImageMetadata(context.Background(), "narumq", "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq:latest", "narumq-ecr"); err == nil {
		t.Fatal("mutable tag was accepted")
	}

	// 2. Rejects unallowlisted registries
	unallowed := "docker.io/library/narumq@sha256:" + strings.Repeat("c", 64)
	if _, err := reader.ReadImageMetadata(context.Background(), "narumq", unallowed, "narumq-ecr"); err == nil {
		t.Fatal("unallowlisted registry was accepted")
	}

	// 3. Rejects missing Secret
	allowed := "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@sha256:" + strings.Repeat("c", 64)
	if _, err := reader.ReadImageMetadata(context.Background(), "narumq", allowed, "absent-secret"); err == nil {
		t.Fatal("absent secret was accepted")
	}

	// 4. Memory cache operates without leaking across entries and bounds to maxMetadataDigests
	for index := range maxMetadataDigests + 2 {
		digest := fmt.Sprintf("sha256:%064x", index+1)
		reader.cache(digest, rolloutapp.ImageMetadataDTO{
			Image: "cached", ManifestDigest: digest, OS: "linux", Architecture: "arm64",
			Version: "1.2.0", Features: []string{"compatibility-probe"},
		})
	}
	if len(reader.verified) > maxMetadataDigests {
		t.Fatalf("cache size %d exceeded bound %d", len(reader.verified), maxMetadataDigests)
	}
}

func TestOCICacheStillRequiresSameNamespaceRegistryCredentials(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	registry := "registry.test"
	digest := "sha256:" + strings.Repeat("a", 64)
	image := registry + "/narumq@" + digest
	objects := []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "valid"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.test":{"auth":"dXNlcjpwYXNz"}}}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "wrong-type"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.test":{"auth":"dXNlcjpwYXNz"}}}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "wrong-registry"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"other.test":{"auth":"dXNlcjpwYXNz"}}}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "malformed-entry"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.test":"invalid"}}`)},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	reader, err := NewOCIImageMetadataReader(fakeClient, []string{"https://registry.test"})
	if err != nil {
		t.Fatal(err)
	}
	reader.cache(digest, rolloutapp.ImageMetadataDTO{
		Image: image, ManifestDigest: digest, OS: "linux", Architecture: "amd64", Version: "1.2.0",
		Features: []string{"compatibility-probe"},
	})
	if _, err := reader.ReadImageMetadata(context.Background(), "ns", image, "valid"); err != nil {
		t.Fatalf("valid Secret should permit cached immutable metadata: %v", err)
	}
	for _, name := range []string{"missing", "wrong-type", "wrong-registry", "malformed-entry"} {
		if _, err := reader.ReadImageMetadata(context.Background(), "ns", image, name); err == nil {
			t.Errorf("cached digest bypassed invalid registry Secret %q", name)
		}
	}
	if _, err := reader.ReadImageMetadata(context.Background(), "other-ns", image, "valid"); err == nil {
		t.Error("cached digest bypassed same-namespace Secret boundary")
	}
}
