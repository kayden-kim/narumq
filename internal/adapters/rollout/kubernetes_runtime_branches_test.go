package rollout

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	rolloutapp "narumq/internal/application/rollout"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var errRuntimeAPI = errors.New("kubernetes api unavailable")

var rtLeaseResource = schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}

var rtClaim = rolloutapp.TargetClaimDTO{Namespace: "ns", Workload: "broker", RolloutUID: "uid-1", Rollout: "roll-1"}

func rtScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("register scheme: %v", err)
		}
	}
	return scheme
}

func rtRuntime(t *testing.T, funcs interceptor.Funcs, objects ...client.Object) (*KubernetesRuntime, client.Client) {
	t.Helper()
	kube := fake.NewClientBuilder().WithScheme(rtScheme(t)).WithObjects(objects...).WithInterceptorFuncs(funcs).Build()
	adapter, err := NewKubernetesRuntime(kube, kube)
	if err != nil {
		t.Fatalf("NewKubernetesRuntime: %v", err)
	}
	return adapter, kube
}

func rtLeaseKey(claim rolloutapp.TargetClaimDTO) types.NamespacedName {
	return types.NamespacedName{Namespace: claim.Namespace, Name: targetLeaseName(claim.Workload)}
}

func rtLease(claim rolloutapp.TargetClaimDTO, holder *string, annotatedWorkload string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: claim.Namespace, Name: targetLeaseName(claim.Workload),
			Annotations: map[string]string{"narumq.io/workload": annotatedWorkload},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: holder},
	}
}

func TestNewKubernetesRuntimeRequiresBothClients(t *testing.T) {
	kube := fake.NewClientBuilder().Build()
	if _, err := NewKubernetesRuntime(nil, kube); err == nil {
		t.Error("nil write client was accepted")
	}
	if _, err := NewKubernetesRuntime(kube, nil); err == nil {
		t.Error("nil uncached API reader was accepted")
	}
	if _, err := NewKubernetesRuntime(kube, kube); err != nil {
		t.Errorf("valid clients rejected: %v", err)
	}
}

func TestKubernetesRuntimeClaimAndReleaseRejectInvalidIdentityWithoutSideEffects(t *testing.T) {
	claims := map[string]rolloutapp.TargetClaimDTO{
		"empty namespace":         {Workload: "broker", RolloutUID: "uid-1", Rollout: "roll-1"},
		"empty workload":          {Namespace: "ns", RolloutUID: "uid-1", Rollout: "roll-1"},
		"empty rollout uid":       {Namespace: "ns", Workload: "broker", Rollout: "roll-1"},
		"empty rollout name":      {Namespace: "ns", Workload: "broker", RolloutUID: "uid-1"},
		"namespace not a label":   {Namespace: "Bad_NS", Workload: "broker", RolloutUID: "uid-1", Rollout: "roll-1"},
		"workload not a hostname": {Namespace: "ns", Workload: "Bad_Name", RolloutUID: "uid-1", Rollout: "roll-1"},
	}
	for name, claim := range claims {
		t.Run(name, func(t *testing.T) {
			writes := 0
			count := func() { writes++ }
			adapter, kube := rtRuntime(t, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					count()
					return c.Create(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					count()
					return c.Delete(ctx, obj, opts...)
				},
			})
			claimed, err := adapter.ClaimTarget(context.Background(), claim)
			if err == nil || claimed {
				t.Fatalf("ClaimTarget = %v, %v; want rejection", claimed, err)
			}
			if err := adapter.ReleaseTarget(context.Background(), claim); err == nil {
				t.Fatal("ReleaseTarget accepted invalid identity")
			}
			leases := &coordinationv1.LeaseList{}
			if err := kube.List(context.Background(), leases); err != nil {
				t.Fatal(err)
			}
			if writes != 0 || len(leases.Items) != 0 {
				t.Fatalf("invalid identity caused writes=%d leases=%d", writes, len(leases.Items))
			}
		})
	}
}

func TestKubernetesRuntimeClaimTargetCreatesThenOnlyRenewsForTheOwner(t *testing.T) {
	adapter, kube := rtRuntime(t, interceptor.Funcs{})
	ctx := context.Background()

	claimed, err := adapter.ClaimTarget(ctx, rtClaim)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	first := &coordinationv1.Lease{}
	if err := kube.Get(ctx, rtLeaseKey(rtClaim), first); err != nil {
		t.Fatalf("lease was not created: %v", err)
	}
	if first.Spec.HolderIdentity == nil || *first.Spec.HolderIdentity != "uid-1" ||
		first.Annotations["narumq.io/workload"] != "broker" || first.Annotations["narumq.io/rollout"] != "roll-1" ||
		first.Spec.LeaseDurationSeconds == nil || first.Spec.RenewTime == nil || first.Spec.AcquireTime == nil {
		t.Fatalf("created lease does not carry ownership evidence: %+v", first)
	}

	claimed, err = adapter.ClaimTarget(ctx, rtClaim)
	if err != nil || !claimed {
		t.Fatalf("owner renewal = %v, %v", claimed, err)
	}
	renewed := &coordinationv1.Lease{}
	if err := kube.Get(ctx, rtLeaseKey(rtClaim), renewed); err != nil {
		t.Fatal(err)
	}
	if renewed.ResourceVersion == first.ResourceVersion || renewed.Spec.RenewTime.Time.Before(first.Spec.RenewTime.Time) {
		t.Fatalf("owner claim did not renew the lease: before=%+v after=%+v", first.Spec, renewed.Spec)
	}
	if renewed.Spec.HolderIdentity == nil || *renewed.Spec.HolderIdentity != "uid-1" {
		t.Fatalf("renewal changed the holder: %+v", renewed.Spec)
	}

	other := rtClaim
	other.RolloutUID = "uid-2"
	other.Rollout = "roll-2"
	claimed, err = adapter.ClaimTarget(ctx, other)
	if err != nil || claimed {
		t.Fatalf("competing rollout claim = %v, %v; want denied without error", claimed, err)
	}
	after := &coordinationv1.Lease{}
	if err := kube.Get(ctx, rtLeaseKey(rtClaim), after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != renewed.ResourceVersion || *after.Spec.HolderIdentity != "uid-1" {
		t.Fatalf("competing claim modified the lease: %+v", after)
	}
}

func TestKubernetesRuntimeClaimTargetDeniesLeasesNotProvablyOwned(t *testing.T) {
	cases := map[string]*coordinationv1.Lease{
		"holder unset":             rtLease(rtClaim, nil, "broker"),
		"workload annotation lies": rtLease(rtClaim, new("uid-1"), "other-workload"),
		"annotation missing":       rtLease(rtClaim, new("uid-1"), ""),
	}
	for name, lease := range cases {
		t.Run(name, func(t *testing.T) {
			adapter, kube := rtRuntime(t, interceptor.Funcs{}, lease)
			before := &coordinationv1.Lease{}
			if err := kube.Get(context.Background(), rtLeaseKey(rtClaim), before); err != nil {
				t.Fatal(err)
			}
			claimed, err := adapter.ClaimTarget(context.Background(), rtClaim)
			if err != nil || claimed {
				t.Fatalf("ClaimTarget = %v, %v; want denied", claimed, err)
			}
			after := &coordinationv1.Lease{}
			if err := kube.Get(context.Background(), rtLeaseKey(rtClaim), after); err != nil {
				t.Fatal(err)
			}
			if after.ResourceVersion != before.ResourceVersion {
				t.Fatal("denied claim still wrote the lease")
			}
		})
	}
}

func TestKubernetesRuntimeClaimTargetClassifiesAPIFailures(t *testing.T) {
	cases := []struct {
		name        string
		objects     []client.Object
		funcs       interceptor.Funcs
		wantClaimed bool
		wantErr     error
	}{
		{
			name: "read failure is surfaced and nothing is created",
			funcs: interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errRuntimeAPI
			}},
			wantErr: errRuntimeAPI,
		},
		{
			name: "lost create race to AlreadyExists is a denial",
			funcs: interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(rtLeaseResource, "lease")
			}},
		},
		{
			name: "lost create race to Conflict is a denial",
			funcs: interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return apierrors.NewConflict(rtLeaseResource, "lease", errors.New("raced"))
			}},
		},
		{
			name: "other create failure is surfaced",
			funcs: interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return errRuntimeAPI
			}},
			wantErr: errRuntimeAPI,
		},
		{
			name:    "renewal conflict is a denial",
			objects: []client.Object{rtLease(rtClaim, new("uid-1"), "broker")},
			funcs: interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return apierrors.NewConflict(rtLeaseResource, "lease", errors.New("raced"))
			}},
		},
		{
			name:    "other renewal failure is surfaced",
			objects: []client.Object{rtLease(rtClaim, new("uid-1"), "broker")},
			funcs: interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return errRuntimeAPI
			}},
			wantErr: errRuntimeAPI,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter, _ := rtRuntime(t, tc.funcs, tc.objects...)
			claimed, err := adapter.ClaimTarget(context.Background(), rtClaim)
			if claimed != tc.wantClaimed {
				t.Fatalf("claimed = %v, want %v (err=%v)", claimed, tc.wantClaimed, err)
			}
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestKubernetesRuntimeReleaseTargetOnlyDeletesOwnedLease(t *testing.T) {
	ctx := context.Background()

	t.Run("absent lease is already released", func(t *testing.T) {
		adapter, _ := rtRuntime(t, interceptor.Funcs{})
		if err := adapter.ReleaseTarget(ctx, rtClaim); err != nil {
			t.Fatalf("ReleaseTarget on absent lease: %v", err)
		}
	})

	t.Run("read failure is surfaced", func(t *testing.T) {
		adapter, _ := rtRuntime(t, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errRuntimeAPI
		}})
		if err := adapter.ReleaseTarget(ctx, rtClaim); !errors.Is(err, errRuntimeAPI) {
			t.Fatalf("error = %v, want %v", err, errRuntimeAPI)
		}
	})

	for name, holder := range map[string]*string{"another rollout holds it": new("uid-2"), "holder unset": nil} {
		t.Run(name, func(t *testing.T) {
			adapter, kube := rtRuntime(t, interceptor.Funcs{}, rtLease(rtClaim, holder, "broker"))
			if err := adapter.ReleaseTarget(ctx, rtClaim); err == nil {
				t.Fatal("release of a lease held by someone else succeeded")
			}
			if err := kube.Get(ctx, rtLeaseKey(rtClaim), &coordinationv1.Lease{}); err != nil {
				t.Fatalf("foreign lease was deleted: %v", err)
			}
		})
	}

	t.Run("owner release deletes", func(t *testing.T) {
		adapter, kube := rtRuntime(t, interceptor.Funcs{}, rtLease(rtClaim, new("uid-1"), "broker"))
		if err := adapter.ReleaseTarget(ctx, rtClaim); err != nil {
			t.Fatalf("ReleaseTarget: %v", err)
		}
		if err := kube.Get(ctx, rtLeaseKey(rtClaim), &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
			t.Fatalf("lease still present after release: %v", err)
		}
	})

	t.Run("lease changed between read and delete is not deleted", func(t *testing.T) {
		raced := false
		adapter, kube := rtRuntime(t, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				lease, ok := obj.(*coordinationv1.Lease)
				if !ok || raced {
					return nil
				}
				raced = true
				// A concurrent writer bumps the resourceVersion after our read.
				concurrent := lease.DeepCopy()
				concurrent.Annotations["concurrent"] = "write"
				return c.Update(ctx, concurrent)
			},
		}, rtLease(rtClaim, new("uid-1"), "broker"))
		err := adapter.ReleaseTarget(ctx, rtClaim)
		if !apierrors.IsConflict(err) {
			t.Fatalf("ReleaseTarget error = %v, want Conflict from the resourceVersion precondition", err)
		}
		if err := kube.Get(ctx, rtLeaseKey(rtClaim), &coordinationv1.Lease{}); err != nil {
			t.Fatalf("lease was deleted despite a stale precondition: %v", err)
		}
	})
}

func TestKubernetesRuntimeDeletePodRequiresAllPreconditions(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "broker-0", UID: "pod-uid"}}
	podKey := types.NamespacedName{Namespace: "ns", Name: "broker-0"}

	current := func(t *testing.T, kube client.Client) string {
		t.Helper()
		stored := &corev1.Pod{}
		if err := kube.Get(ctx, podKey, stored); err != nil {
			t.Fatal(err)
		}
		return stored.ResourceVersion
	}

	incomplete := map[string]rolloutapp.DeletePodDTO{
		"namespace":       {Name: "broker-0", UID: "pod-uid", ResourceVersion: "1"},
		"name":            {Namespace: "ns", UID: "pod-uid", ResourceVersion: "1"},
		"uid":             {Namespace: "ns", Name: "broker-0", ResourceVersion: "1"},
		"resourceVersion": {Namespace: "ns", Name: "broker-0", UID: "pod-uid"},
	}
	for missing, request := range incomplete {
		t.Run("missing "+missing, func(t *testing.T) {
			adapter, kube := rtRuntime(t, interceptor.Funcs{}, pod.DeepCopy())
			if err := adapter.DeletePod(ctx, request); err == nil {
				t.Fatal("unconditional Pod delete was issued")
			}
			if err := kube.Get(ctx, podKey, &corev1.Pod{}); err != nil {
				t.Fatalf("Pod was deleted without full preconditions: %v", err)
			}
		})
	}

	t.Run("stale resourceVersion is rejected", func(t *testing.T) {
		adapter, kube := rtRuntime(t, interceptor.Funcs{}, pod.DeepCopy())
		err := adapter.DeletePod(ctx, rolloutapp.DeletePodDTO{Namespace: "ns", Name: "broker-0", UID: "pod-uid", ResourceVersion: current(t, kube) + "0"})
		if !apierrors.IsConflict(err) {
			t.Fatalf("DeletePod error = %v, want Conflict", err)
		}
		if err := kube.Get(ctx, podKey, &corev1.Pod{}); err != nil {
			t.Fatalf("Pod deleted despite stale precondition: %v", err)
		}
	})

	t.Run("matching preconditions delete exactly that pod instance", func(t *testing.T) {
		var sent *metav1.Preconditions
		adapter, kube := rtRuntime(t, interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				var applied client.DeleteOptions
				applied.ApplyOptions(opts)
				sent = applied.Preconditions
				return c.Delete(ctx, obj, opts...)
			},
		}, pod.DeepCopy())
		rv := current(t, kube)
		if err := adapter.DeletePod(ctx, rolloutapp.DeletePodDTO{Namespace: "ns", Name: "broker-0", UID: "pod-uid", ResourceVersion: rv}); err != nil {
			t.Fatalf("DeletePod: %v", err)
		}
		if sent == nil || sent.UID == nil || *sent.UID != "pod-uid" || sent.ResourceVersion == nil || *sent.ResourceVersion != rv {
			t.Fatalf("delete preconditions = %+v, want UID and resourceVersion pinned", sent)
		}
		if err := kube.Get(ctx, podKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("Pod still present: %v", err)
		}
	})
}

func TestPodOrdinalAcceptsOnlyCanonicalSupportedOrdinals(t *testing.T) {
	cases := []struct {
		pod     string
		want    int32
		wantErr bool
	}{
		{pod: "broker-0", want: 0},
		{pod: "broker-2", want: 2},
		{pod: "other-0", wantErr: true},
		{pod: "broker", wantErr: true},
		{pod: "broker-", wantErr: true},
		{pod: "broker-x", wantErr: true},
		{pod: "broker-00", wantErr: true},
		{pod: "broker-+1", wantErr: true},
		{pod: "broker--1", wantErr: true},
		{pod: "broker-3", wantErr: true},
		{pod: "broker-99999999999", wantErr: true},
	}
	for _, tc := range cases {
		got, err := podOrdinal("broker", tc.pod)
		if tc.wantErr {
			if err == nil {
				t.Errorf("podOrdinal(%q) = %d, want error", tc.pod, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("podOrdinal(%q) = %d, %v; want %d", tc.pod, got, err, tc.want)
		}
	}
}

func TestHasStatefulSetOwnerRequiresControllingStatefulSetWithMatchingUID(t *testing.T) {
	owner := func(uid types.UID, kind string, controller *bool) metav1.OwnerReference {
		return metav1.OwnerReference{UID: uid, Kind: kind, Name: "broker", APIVersion: "apps/v1", Controller: controller}
	}
	cases := []struct {
		name   string
		owners []metav1.OwnerReference
		want   bool
	}{
		{"controller StatefulSet with matching UID", []metav1.OwnerReference{owner("sts", "StatefulSet", new(true))}, true},
		{"matching owner found after unrelated owner", []metav1.OwnerReference{owner("rs", "ReplicaSet", new(true)), owner("sts", "StatefulSet", new(true))}, true},
		{"no owners", nil, false},
		{"different UID", []metav1.OwnerReference{owner("other", "StatefulSet", new(true))}, false},
		{"different kind", []metav1.OwnerReference{owner("sts", "ReplicaSet", new(true))}, false},
		{"controller flag unset", []metav1.OwnerReference{owner("sts", "StatefulSet", nil)}, false},
		{"controller flag false", []metav1.OwnerReference{owner("sts", "StatefulSet", new(false))}, false},
	}
	for _, tc := range cases {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: tc.owners}}
		if got := hasStatefulSetOwner(pod, "sts"); got != tc.want {
			t.Errorf("%s: hasStatefulSetOwner = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestServiceSelectsPodRequiresEverySelectorLabel(t *testing.T) {
	service := &corev1.Service{Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "narumq", "tier": "broker"}}}
	if err := serviceSelectsPod(service, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "narumq", "tier": "broker", "extra": "x"}}}); err != nil {
		t.Errorf("Pod with all selector labels rejected: %v", err)
	}
	for name, labels := range map[string]map[string]string{
		"missing label": {"app": "narumq"},
		"wrong value":   {"app": "narumq", "tier": "other"},
		"no labels":     nil,
	} {
		if err := serviceSelectsPod(service, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels}}); err == nil {
			t.Errorf("%s: Pod outside the Service selector was accepted", name)
		}
	}
}

func rtStatefulSetWithContainerPort(name string, port int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "narumq", Ports: []corev1.ContainerPort{{Name: name, ContainerPort: port}}}},
	}}}}
}

func rtManagementService(ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Selector: map[string]string{"app": "narumq"}, Ports: ports}}
}

func rtMgmtPort(target intstr.IntOrString) corev1.ServicePort {
	return corev1.ServicePort{Name: "http-mgmt", Port: 8080, Protocol: corev1.ProtocolTCP, TargetPort: target}
}

func TestValidateManagementServiceRequiresClusterIPSelectorAndTCP8080ToNaruMQ(t *testing.T) {
	mgmtSTS := rtStatefulSetWithContainerPort("mgmt", 8080)
	cases := []struct {
		name    string
		service *corev1.Service
		sts     *appsv1.StatefulSet
		wantErr bool
	}{
		{"numeric targetPort 8080", rtManagementService(rtMgmtPort(intstr.FromInt32(8080))), mgmtSTS, false},
		{"named targetPort resolving to container port 8080", rtManagementService(rtMgmtPort(intstr.FromString("mgmt"))), mgmtSTS, false},
		{"valid port listed after unrelated ports", rtManagementService(
			corev1.ServicePort{Name: "metrics", Port: 9090, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(9090)},
			rtMgmtPort(intstr.FromInt32(8080)),
		), mgmtSTS, false},
		{"named targetPort without a matching container port name", rtManagementService(rtMgmtPort(intstr.FromString("other"))), mgmtSTS, true},
		{"named targetPort resolving to a different container port", rtManagementService(rtMgmtPort(intstr.FromString("mgmt"))), rtStatefulSetWithContainerPort("mgmt", 9090), true},
		{"numeric targetPort to another port", rtManagementService(rtMgmtPort(intstr.FromInt32(9090))), mgmtSTS, true},
		{"port named differently", rtManagementService(corev1.ServicePort{Name: "http", Port: 8080, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)}), mgmtSTS, true},
		{"service port other than 8080", rtManagementService(corev1.ServicePort{Name: "http-mgmt", Port: 9090, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)}), mgmtSTS, true},
		{"UDP protocol", rtManagementService(corev1.ServicePort{Name: "http-mgmt", Port: 8080, Protocol: corev1.ProtocolUDP, TargetPort: intstr.FromInt32(8080)}), mgmtSTS, true},
		{"no ports", rtManagementService(), mgmtSTS, true},
		{"empty ClusterIP", func() *corev1.Service {
			service := rtManagementService(rtMgmtPort(intstr.FromInt32(8080)))
			service.Spec.ClusterIP = ""
			return service
		}(), mgmtSTS, true},
		{"headless Service", func() *corev1.Service {
			service := rtManagementService(rtMgmtPort(intstr.FromInt32(8080)))
			service.Spec.ClusterIP = corev1.ClusterIPNone
			return service
		}(), mgmtSTS, true},
		{"Service without selector", func() *corev1.Service {
			service := rtManagementService(rtMgmtPort(intstr.FromInt32(8080)))
			service.Spec.Selector = nil
			return service
		}(), mgmtSTS, true},
	}
	for _, tc := range cases {
		err := validateManagementService(tc.service, tc.sts)
		if tc.wantErr && err == nil {
			t.Errorf("%s: invalid management Service was accepted", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: valid management Service rejected: %v", tc.name, err)
		}
	}
}

type rtWorkloadFixture struct {
	statefulSet *appsv1.StatefulSet
	service     *corev1.Service
	pods        []*corev1.Pod
	nodes       []*corev1.Node
	extra       []client.Object
}

func newRTWorkloadFixture() *rtWorkloadFixture {
	const narumqImage = "registry.test/narumq@sha256:old"
	fixture := &rtWorkloadFixture{
		statefulSet: &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "broker", UID: "sts-uid", Generation: 4},
			Spec: appsv1.StatefulSetSpec{
				Replicas:       new(int32(3)),
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "narumq"}},
				UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "narumq", Image: narumqImage, Ports: []corev1.ContainerPort{{Name: "mgmt", ContainerPort: 8080}}},
						{Name: "sidecar", Image: "sidecar:1"},
					},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull"}},
				}},
			},
			Status: appsv1.StatefulSetStatus{ObservedGeneration: 3},
		},
		service: &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "narumq-management"},
			Spec: corev1.ServiceSpec{
				ClusterIP: "10.0.0.1", Selector: map[string]string{"app": "narumq"},
				Ports: []corev1.ServicePort{rtMgmtPort(intstr.FromString("mgmt"))},
			},
		},
		nodes: []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{
			Name: "node-a", Labels: map[string]string{corev1.LabelOSStable: "linux", corev1.LabelArchStable: "arm64"},
		}}},
	}
	controller := true
	for ordinal := range 3 {
		fixture.pods = append(fixture.pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: fmt.Sprintf("broker-%d", ordinal), UID: types.UID(fmt.Sprintf("pod-uid-%d", ordinal)),
				Labels: map[string]string{"app": "narumq"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "StatefulSet", Name: "broker", UID: "sts-uid", Controller: &controller,
				}},
			},
			Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "narumq", Image: narumqImage}}},
			Status: corev1.PodStatus{
				PodIP:             fmt.Sprintf("10.1.0.%d", ordinal),
				ContainerStatuses: []corev1.ContainerStatus{{Name: "narumq", ImageID: "registry.test/narumq@sha256:imageid"}},
				Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		})
	}
	return fixture
}

func (fixture *rtWorkloadFixture) objects() []client.Object {
	var objects []client.Object
	if fixture.statefulSet != nil {
		objects = append(objects, fixture.statefulSet)
	}
	if fixture.service != nil {
		objects = append(objects, fixture.service)
	}
	for _, pod := range fixture.pods {
		objects = append(objects, pod)
	}
	for _, node := range fixture.nodes {
		objects = append(objects, node)
	}
	return append(objects, fixture.extra...)
}

func (fixture *rtWorkloadFixture) inspect(t *testing.T, funcs interceptor.Funcs) (rolloutapp.WorkloadDTO, error) {
	t.Helper()
	adapter, _ := rtRuntime(t, funcs, fixture.objects()...)
	return adapter.InspectWorkload(context.Background(), rolloutapp.RolloutDTO{Namespace: "ns", WorkloadName: "broker"})
}

func TestKubernetesRuntimeInspectWorkloadReportsOwnedPodsAndPlatformEvidence(t *testing.T) {
	fixture := newRTWorkloadFixture()
	fixture.pods[0].Spec.Containers = append(fixture.pods[0].Spec.Containers, corev1.Container{Name: "sidecar", Image: "sidecar:1"})
	fixture.pods[0].Status.ContainerStatuses = append(fixture.pods[0].Status.ContainerStatuses, corev1.ContainerStatus{Name: "sidecar", ImageID: "sidecar-image-id"})
	fixture.pods[1].Finalizers = []string{"test/hold"}
	fixture.pods[1].DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)}
	fixture.pods[2].Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	fixture.extra = []client.Object{
		// Same name in another namespace and a selector miss in this one must never be reported as rollout Pods.
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "broker-0", Labels: map[string]string{"app": "narumq"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "unrelated-0", Labels: map[string]string{"app": "other"}}},
	}

	workload, err := fixture.inspect(t, interceptor.Funcs{})
	if err != nil {
		t.Fatalf("InspectWorkload: %v", err)
	}
	if workload.Name != "broker" || workload.UID != "sts-uid" || workload.Replicas != 3 || workload.UpdateStrategy != "OnDelete" ||
		workload.Generation != 4 || workload.ObservedGeneration != 3 || workload.ResourceVersion == "" {
		t.Fatalf("workload identity = %+v", workload)
	}
	if len(workload.Containers) != 2 || workload.Containers[0].Name != "narumq" || workload.Containers[0].Image != "registry.test/narumq@sha256:old" || workload.Containers[1].Name != "sidecar" {
		t.Fatalf("containers = %+v", workload.Containers)
	}
	if len(workload.ImagePullSecrets) != 1 || workload.ImagePullSecrets[0] != "pull" {
		t.Fatalf("imagePullSecrets = %v", workload.ImagePullSecrets)
	}
	if len(workload.Pods) != 3 {
		t.Fatalf("pods = %+v, want exactly the three owned ordinals", workload.Pods)
	}
	byName := map[string]rolloutapp.PodDTO{}
	for _, pod := range workload.Pods {
		byName[pod.Name] = pod
	}
	for ordinal := range 3 {
		pod, ok := byName[fmt.Sprintf("broker-%d", ordinal)]
		if !ok {
			t.Fatalf("pod broker-%d missing from %+v", ordinal, workload.Pods)
		}
		if pod.Ordinal != int32(ordinal) || pod.UID != fmt.Sprintf("pod-uid-%d", ordinal) || pod.PodIP != fmt.Sprintf("10.1.0.%d", ordinal) ||
			pod.Image != "registry.test/narumq@sha256:old" || pod.ImageID != "registry.test/narumq@sha256:imageid" ||
			pod.OS != "linux" || pod.Architecture != "arm64" || pod.ResourceVersion == "" {
			t.Errorf("pod broker-%d = %+v", ordinal, pod)
		}
	}
	if !byName["broker-0"].Ready || byName["broker-0"].Deleting {
		t.Errorf("broker-0 ready/deleting = %v/%v, want true/false", byName["broker-0"].Ready, byName["broker-0"].Deleting)
	}
	if !byName["broker-1"].Ready || !byName["broker-1"].Deleting {
		t.Errorf("broker-1 ready/deleting = %v/%v, want true/true", byName["broker-1"].Ready, byName["broker-1"].Deleting)
	}
	if byName["broker-2"].Ready || byName["broker-2"].Deleting {
		t.Errorf("broker-2 ready/deleting = %v/%v, want false/false", byName["broker-2"].Ready, byName["broker-2"].Deleting)
	}
}

func TestKubernetesRuntimeInspectWorkloadTreatsUnsetReplicasAsZero(t *testing.T) {
	fixture := newRTWorkloadFixture()
	fixture.statefulSet.Spec.Replicas = nil
	workload, err := fixture.inspect(t, interceptor.Funcs{})
	if err != nil {
		t.Fatalf("InspectWorkload: %v", err)
	}
	if workload.Replicas != 0 {
		t.Fatalf("Replicas = %d, want 0 for an unset spec.replicas", workload.Replicas)
	}
}

func TestKubernetesRuntimeInspectWorkloadRejectsUnsafeTopologyWithoutPartialEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*rtWorkloadFixture)
		want   string
	}{
		{"empty selector would match every Pod", func(f *rtWorkloadFixture) { f.statefulSet.Spec.Selector = &metav1.LabelSelector{} }, "selector is invalid"},
		{"malformed selector expression", func(f *rtWorkloadFixture) {
			f.statefulSet.Spec.Selector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: "Bogus"}}}
		}, "selector is invalid"},
		{"management Service absent", func(f *rtWorkloadFixture) { f.service = nil }, "unavailable"},
		{"management Service headless", func(f *rtWorkloadFixture) { f.service.Spec.ClusterIP = corev1.ClusterIPNone }, "ClusterIP"},
		{"management Service does not expose 8080", func(f *rtWorkloadFixture) { f.service.Spec.Ports[0].Port = 9090 }, "TCP port 8080"},
		{"more Pods than ordinals", func(f *rtWorkloadFixture) {
			extra := f.pods[2].DeepCopy()
			extra.Name, extra.UID = "broker-3", "pod-uid-3"
			f.pods = append(f.pods, extra)
		}, "more Pods"},
		{"Pod name from another StatefulSet", func(f *rtWorkloadFixture) { f.pods[0].Name = "other-0" }, "stable ordinal"},
		{"Pod ordinal not canonical", func(f *rtWorkloadFixture) { f.pods[0].Name = "broker-00" }, "stable ordinal"},
		{"Pod without controller owner", func(f *rtWorkloadFixture) { f.pods[1].OwnerReferences = nil }, "stable ordinal"},
		{"Pod controlled by a different UID", func(f *rtWorkloadFixture) { f.pods[1].OwnerReferences[0].UID = "someone-else" }, "stable ordinal"},
		{"Pod without NaruMQ container", func(f *rtWorkloadFixture) { f.pods[0].Spec.Containers[0].Name = "other" }, "exactly one NaruMQ container"},
		{"Pod with two NaruMQ containers", func(f *rtWorkloadFixture) {
			f.pods[0].Spec.Containers = append(f.pods[0].Spec.Containers, corev1.Container{Name: "narumq", Image: "dup"})
		}, "exactly one NaruMQ container"},
		{"Pod NaruMQ container without image", func(f *rtWorkloadFixture) { f.pods[0].Spec.Containers[0].Image = "" }, "exactly one NaruMQ container"},
		{"Pod outside the management Service selector", func(f *rtWorkloadFixture) { f.service.Spec.Selector["tier"] = "mgmt" }, "does not include every rollout Pod"},
		{"Pod not scheduled", func(f *rtWorkloadFixture) { f.pods[0].Spec.NodeName = "" }, "node platform is unavailable"},
		{"Pod node object missing", func(f *rtWorkloadFixture) { f.pods[0].Spec.NodeName = "ghost" }, "node platform is unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRTWorkloadFixture()
			tc.mutate(fixture)
			workload, err := fixture.inspect(t, interceptor.Funcs{})
			if err == nil {
				t.Fatalf("unsafe topology accepted: %+v", workload)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.want)
			}
			if workload.Name != "" || len(workload.Pods) != 0 {
				t.Fatalf("partial evidence returned with error: %+v", workload)
			}
		})
	}
}

func TestKubernetesRuntimeInspectWorkloadValidatesRequestAndPropagatesAPIErrors(t *testing.T) {
	adapter, _ := rtRuntime(t, interceptor.Funcs{})
	for name, rollout := range map[string]rolloutapp.RolloutDTO{
		"no namespace":     {WorkloadName: "broker"},
		"no workload name": {Namespace: "ns"},
	} {
		if _, err := adapter.InspectWorkload(context.Background(), rollout); err == nil {
			t.Errorf("%s: request accepted", name)
		}
	}
	if _, err := adapter.InspectWorkload(context.Background(), rolloutapp.RolloutDTO{Namespace: "ns", WorkloadName: "missing"}); !apierrors.IsNotFound(err) {
		t.Errorf("missing StatefulSet error = %v, want NotFound to be preserved", err)
	}

	fixture := newRTWorkloadFixture()
	_, err := fixture.inspect(t, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			return errRuntimeAPI
		}
		return c.List(ctx, list, opts...)
	}})
	if !errors.Is(err, errRuntimeAPI) {
		t.Fatalf("Pod list failure = %v, want %v", err, errRuntimeAPI)
	}
}
