package rollout

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"narumq/internal/application/membership"
	rolloutapp "narumq/internal/application/rollout"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	ociDockerManifestType = "application/vnd.docker.distribution.manifest.v2+json"
	ociIndexType          = "application/vnd.oci.image.index.v1+json"
	ociConfigType         = "application/vnd.docker.container.image.v1+json"
)

// ociFakeRegistry serves one manifest and a set of blobs over the Docker registry v2 HTTP API.
type ociFakeRegistry struct {
	manifest         []byte
	mediaType        string
	manifestStatus   int
	blobs            map[string][]byte
	manifestRequests atomic.Int32
}

func (registry *ociFakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v2/":
		w.WriteHeader(http.StatusOK)
	case strings.Contains(r.URL.Path, "/manifests/"):
		registry.manifestRequests.Add(1)
		if registry.manifestStatus != 0 {
			w.WriteHeader(registry.manifestStatus)
			return
		}
		w.Header().Set("Content-Type", registry.mediaType)
		w.Header().Set("Docker-Content-Digest", digestBytes(registry.manifest))
		_, _ = w.Write(registry.manifest)
	case strings.Contains(r.URL.Path, "/blobs/"):
		body, ok := registry.blobs[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

type ociFixtureOptions struct {
	config         []byte // image config JSON; defaults to a valid arm64 config
	blob           []byte // bytes actually served for the config blob; defaults to config
	manifest       []byte // manifest JSON; defaults to a schema-2 manifest referencing config
	mediaType      string // manifest Content-Type; defaults to the Docker v2 manifest type
	manifestStatus int
}

type ociFixture struct {
	reader         *OCIImageMetadataReader
	registry       *ociFakeRegistry
	host           string
	image          string
	manifestDigest string
	configDigest   string
}

func ociCanonicalFeatureLabel(t *testing.T) string {
	t.Helper()
	features, err := membership.BuildFeatures("")
	if err != nil {
		t.Fatalf("BuildFeatures: %v", err)
	}
	return strings.Join(features, ",")
}

func ociConfigJSON(t *testing.T, version, features string) []byte {
	t.Helper()
	config, err := json.Marshal(map[string]any{
		"architecture": "arm64", "os": "linux",
		"config": map[string]any{"Labels": map[string]string{versionLabel: version, featuresLabel: features}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func ociSecret(namespace, secretName, host string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: secretName},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: []byte(fmt.Sprintf(`{"auths":{%q:{"auth":"dXNlcjpwYXNz"}}}`, host)),
		},
	}
}

func ociKube(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(rtScheme(t)).WithObjects(objects...).Build()
}

func newOCIFixture(t *testing.T, options ociFixtureOptions) *ociFixture {
	t.Helper()
	config := options.config
	if config == nil {
		config = ociConfigJSON(t, "1.2.0", ociCanonicalFeatureLabel(t))
	}
	configDigest := digestBytes(config)
	manifest := options.manifest
	if manifest == nil {
		var err error
		manifest, err = json.Marshal(map[string]any{
			"schemaVersion": 2, "mediaType": ociDockerManifestType,
			"config": map[string]any{"mediaType": ociConfigType, "size": len(config), "digest": configDigest},
			"layers": []any{},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	mediaType := options.mediaType
	if mediaType == "" {
		mediaType = ociDockerManifestType
	}
	served := options.blob
	if served == nil {
		served = config
	}
	registry := &ociFakeRegistry{
		manifest: manifest, mediaType: mediaType, manifestStatus: options.manifestStatus,
		blobs: map[string][]byte{configDigest: served},
	}
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	reader, err := NewOCIImageMetadataReader(ociKube(t, ociSecret("ns", "pull", host)), []string{"http://" + host})
	if err != nil {
		t.Fatalf("NewOCIImageMetadataReader: %v", err)
	}
	manifestDigest := digestBytes(manifest)
	return &ociFixture{
		reader: reader, registry: registry, host: host,
		image: host + "/narumq@" + manifestDigest, manifestDigest: manifestDigest, configDigest: configDigest,
	}
}

func TestOCIImageMetadataReaderReturnsVerifiedMetadataAndServesRepeatsFromCache(t *testing.T) {
	fixture := newOCIFixture(t, ociFixtureOptions{})
	want, err := membership.BuildFeatures("")
	if err != nil {
		t.Fatal(err)
	}

	metadata, err := fixture.reader.ReadImageMetadata(context.Background(), "ns", fixture.image, "pull")
	if err != nil {
		t.Fatalf("ReadImageMetadata: %v", err)
	}
	if metadata.Image != fixture.image || metadata.ManifestDigest != fixture.manifestDigest || metadata.ConfigDigest != fixture.configDigest ||
		metadata.OS != "linux" || metadata.Architecture != "arm64" || metadata.Version != "1.2.0" || !slices.Equal(metadata.Features, want) {
		t.Fatalf("metadata = %+v", metadata)
	}
	fetches := fixture.registry.manifestRequests.Load()
	if fetches == 0 {
		t.Fatal("registry was never consulted on a cold read")
	}

	again, err := fixture.reader.ReadImageMetadata(context.Background(), "ns", fixture.image, "pull")
	if err != nil {
		t.Fatalf("cached ReadImageMetadata: %v", err)
	}
	if again.ManifestDigest != metadata.ManifestDigest || again.ConfigDigest != metadata.ConfigDigest || !slices.Equal(again.Features, want) {
		t.Fatalf("cached metadata = %+v, want %+v", again, metadata)
	}
	if got := fixture.registry.manifestRequests.Load(); got != fetches {
		t.Fatalf("immutable digest was fetched again: %d requests, want %d", got, fetches)
	}
}

func TestOCIImageMetadataReaderRejectsRegistryContentThatDoesNotProveTheImage(t *testing.T) {
	canonical := ociCanonicalFeatureLabel(t)
	reversed := strings.Split(canonical, ",")
	slices.Reverse(reversed)

	indexBody := []byte(`{"schemaVersion":2,"mediaType":"` + ociIndexType + `","manifests":[]}`)
	schemaOneManifest := []byte(`{"schemaVersion":1,"mediaType":"` + ociDockerManifestType + `","config":{"mediaType":"` + ociConfigType +
		`","size":2,"digest":"sha256:` + strings.Repeat("a", 64) + `"},"layers":[]}`)

	cases := []struct {
		name    string
		options ociFixtureOptions
	}{
		{"manifest not found", ociFixtureOptions{manifestStatus: http.StatusNotFound}},
		{"digest resolves to an image index", ociFixtureOptions{manifest: indexBody, mediaType: ociIndexType}},
		{"manifest schemaVersion is not 2", ociFixtureOptions{manifest: schemaOneManifest}},
		{"config blob differs from its descriptor", ociFixtureOptions{blob: []byte(`{"architecture":"amd64","os":"linux"}`)}},
		{"config blob is not JSON", ociFixtureOptions{config: []byte("not json")}},
		{"release feature label missing", ociFixtureOptions{config: ociConfigJSON(t, "1.2.0", "")}},
		{"release feature label not canonical", ociFixtureOptions{config: ociConfigJSON(t, "1.2.0", strings.Join(reversed, ","))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newOCIFixture(t, tc.options)
			metadata, err := fixture.reader.ReadImageMetadata(context.Background(), "ns", fixture.image, "pull")
			if err == nil {
				t.Fatalf("unverifiable image accepted: %+v", metadata)
			}
			if metadata.ManifestDigest != "" || len(metadata.Features) != 0 {
				t.Fatalf("metadata returned alongside error: %+v", metadata)
			}
			fixture.reader.mu.Lock()
			cached := len(fixture.reader.verified)
			fixture.reader.mu.Unlock()
			if cached != 0 {
				t.Fatalf("rejected image was cached (%d entries)", cached)
			}
		})
	}
}

func TestOCIImageMetadataReaderRejectsRequestsBeforeAnyRegistryAccess(t *testing.T) {
	fixture := newOCIFixture(t, ociFixtureOptions{})
	reader := fixture.reader
	reader.kube = ociKube(t,
		ociSecret("ns", "pull", fixture.host),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "no-config"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{"other": []byte("x")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "empty-config"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: {}},
		},
	)
	requests := map[string]struct{ namespace, secret string }{
		"no namespace":          {"", "pull"},
		"no secret name":        {"ns", ""},
		"Secret lacks the key":  {"ns", "no-config"},
		"Secret has empty data": {"ns", "empty-config"},
	}
	for name, request := range requests {
		if _, err := reader.ReadImageMetadata(context.Background(), request.namespace, fixture.image, request.secret); err == nil {
			t.Errorf("%s: request accepted", name)
		}
	}
	if got := fixture.registry.manifestRequests.Load(); got != 0 {
		t.Fatalf("registry was contacted %d times for rejected requests", got)
	}
}

func TestOCIImageMetadataReaderAllowlistMatchingIsCaseInsensitive(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	image := "registry.test/narumq@" + digest
	reader, err := NewOCIImageMetadataReader(ociKube(t, ociSecret("ns", "pull", "registry.test")), []string{"https://REGISTRY.Test"})
	if err != nil {
		t.Fatal(err)
	}
	reader.cache(digest, rolloutapp.ImageMetadataDTO{ManifestDigest: digest, Version: "1.2.0"})
	metadata, err := reader.ReadImageMetadata(context.Background(), "ns", image, "pull")
	if err != nil {
		t.Fatalf("mixed-case allowlist entry did not match: %v", err)
	}
	if metadata.Image != image || metadata.ManifestDigest != digest {
		t.Fatalf("metadata = %+v", metadata)
	}
}

func TestNewOCIImageMetadataReaderValidatesAllowlist(t *testing.T) {
	kube := ociKube(t)
	if _, err := NewOCIImageMetadataReader(nil, []string{"https://registry.test"}); err == nil {
		t.Error("nil Secret reader accepted")
	}
	if _, err := NewOCIImageMetadataReader(kube, nil); err == nil {
		t.Error("empty allowlist accepted")
	}
	for _, entry := range []string{
		"https://registry.test/path", "https://registry.test?x=1", "https://registry.test#frag",
		"https://user@registry.test", "https://", "ftp://registry.test", "://bad",
	} {
		if _, err := NewOCIImageMetadataReader(kube, []string{"https://ok.test", entry}); err == nil {
			t.Errorf("allowlist entry %q accepted", entry)
		}
	}

	reader, err := NewOCIImageMetadataReader(kube, []string{"http://Plain.Test:5000", "https://Secure.Test"})
	if err != nil {
		t.Fatal(err)
	}
	if !reader.allowed["plain.test:5000"] || !reader.allowed["secure.test"] {
		t.Fatalf("allowed = %v", reader.allowed)
	}
	if !reader.insecureAllowed["plain.test:5000"] || reader.insecureAllowed["secure.test"] {
		t.Fatalf("insecureAllowed = %v; only explicit http entries may use plain HTTP", reader.insecureAllowed)
	}
}

func TestOCICacheKeepsFirstEntryAndEvictsOldestDigest(t *testing.T) {
	kube := ociKube(t)
	reader, err := NewOCIImageMetadataReader(kube, []string{"https://registry.test"})
	if err != nil {
		t.Fatal(err)
	}
	digest := func(index int) string { return fmt.Sprintf("sha256:%064x", index) }

	reader.cache(digest(1), rolloutapp.ImageMetadataDTO{Version: "first"})
	reader.cache(digest(1), rolloutapp.ImageMetadataDTO{Version: "second"})
	if got := reader.verified[digest(1)].Version; got != "first" || len(reader.order) != 1 {
		t.Fatalf("duplicate digest replaced verified metadata or order: version=%q order=%v", got, reader.order)
	}

	for index := 2; index <= maxMetadataDigests+1; index++ {
		reader.cache(digest(index), rolloutapp.ImageMetadataDTO{Version: fmt.Sprint(index)})
	}
	if _, ok := reader.verified[digest(1)]; ok {
		t.Error("oldest digest survived eviction")
	}
	for index := 2; index <= maxMetadataDigests+1; index++ {
		if _, ok := reader.verified[digest(index)]; !ok {
			t.Errorf("digest %d was evicted early", index)
		}
	}
	if len(reader.verified) != maxMetadataDigests || len(reader.order) != maxMetadataDigests {
		t.Fatalf("cache size verified=%d order=%d, want %d", len(reader.verified), len(reader.order), maxMetadataDigests)
	}
}

func TestNormalizeRegistryKeyCanonicalizesDockerConfigHosts(t *testing.T) {
	cases := map[string]string{
		"":                                   "",
		"   ":                                "",
		"Registry.Test":                      "registry.test",
		"registry.test/":                     "registry.test",
		"  Registry.Test/ ":                  "registry.test",
		"https://Registry.Test/v1/":          "registry.test",
		"https://registry.test:5000":         "registry.test:5000",
		"https://registry.test:5000/v1/":     "registry.test:5000",
		"registry.test:5000":                 "registry.test:5000",
		"http://user@Registry.Test:5000/v1/": "registry.test:5000",
	}
	for input, want := range cases {
		if got := normalizeRegistryKey(input); got != want {
			t.Errorf("normalizeRegistryKey(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestValidateDockerRegistryEntryRequiresDirectCredentialsForTargetRegistry(t *testing.T) {
	cases := []struct {
		name     string
		config   string
		registry string
		wantErr  bool
	}{
		{"exact host", `{"auths":{"registry.test":{}}}`, "registry.test", false},
		{"URL-style key with version suffix", `{"auths":{"https://Registry.Test/v1/":{}}}`, "registry.test", false},
		{"host with port", `{"auths":{"registry.test:5000":{}}}`, "registry.test:5000", false},
		{"match among several entries", `{"auths":{"other.test":{},"registry.test":{}}}`, "registry.test", false},
		{"only another registry", `{"auths":{"other.test":{}}}`, "registry.test", true},
		{"port mismatch", `{"auths":{"registry.test:5000":{}}}`, "registry.test", true},
		{"blank key", `{"auths":{"":{}}}`, "registry.test", true},
		{"no auths", `{}`, "registry.test", true},
		{"empty auths", `{"auths":{}}`, "registry.test", true},
		{"credential helper only", `{"credsStore":"ecr-login"}`, "registry.test", true},
		{"malformed JSON", `not json`, "registry.test", true},
	}
	for _, tc := range cases {
		err := validateDockerRegistryEntry([]byte(tc.config), tc.registry)
		if tc.wantErr && err == nil {
			t.Errorf("%s: config accepted", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: config rejected: %v", tc.name, err)
		}
	}
}

func TestStaticDockerKeychainResolvesOnlyConfiguredRegistries(t *testing.T) {
	keychain, err := newStaticDockerKeychain([]byte(`{"auths":{"https://Registry.Test/v1/":{"username":"u","password":"p"}}}`), "registry.test")
	if err != nil {
		t.Fatalf("newStaticDockerKeychain: %v", err)
	}
	known, err := name.NewRegistry("registry.test")
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := keychain.Resolve(known)
	if err != nil {
		t.Fatalf("Resolve configured registry: %v", err)
	}
	config, err := authenticator.Authorization()
	if err != nil || config.Username != "u" || config.Password != "p" {
		t.Fatalf("Authorization = %+v, %v", config, err)
	}

	unknown, err := name.NewRegistry("other.test")
	if err != nil {
		t.Fatal(err)
	}
	if authenticator, err := keychain.Resolve(unknown); err == nil || authenticator != nil {
		t.Fatalf("credentials were handed to an unconfigured registry: %v, %v", authenticator, err)
	}

	if _, err := newStaticDockerKeychain([]byte("not json"), "registry.test"); err == nil {
		t.Error("malformed Docker config accepted")
	}
	if _, err := newStaticDockerKeychain([]byte(`{"auths":{"other.test":{}}}`), "registry.test"); err == nil {
		t.Error("Docker config without target registry credentials accepted")
	}
}
