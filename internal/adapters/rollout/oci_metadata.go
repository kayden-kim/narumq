package rollout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"narumq/internal/application/membership"
	rolloutapp "narumq/internal/application/rollout"

	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	ocimediatypes "github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	versionLabel       = "org.opencontainers.image.version"
	featuresLabel      = "io.narumq.features"
	maxMetadataDigests = 4
)

type OCIImageMetadataReader struct {
	kube            client.Reader
	allowed         map[string]bool
	insecureAllowed map[string]bool
	mu              sync.Mutex
	verified        map[string]rolloutapp.ImageMetadataDTO
	order           []string
}

func NewOCIImageMetadataReader(kube client.Reader, registries []string) (*OCIImageMetadataReader, error) {
	if kube == nil || len(registries) == 0 {
		return nil, errors.New("Kubernetes Secret reader and explicit OCI registry allowlist are required")
	}
	reader := &OCIImageMetadataReader{
		kube: kube, allowed: make(map[string]bool, len(registries)), insecureAllowed: make(map[string]bool),
		verified: make(map[string]rolloutapp.ImageMetadataDTO, maxMetadataDigests), order: make([]string, 0, maxMetadataDigests),
	}
	for _, configured := range registries {
		parsed, err := url.Parse(configured)
		if err != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil || parsed.Hostname() == "" {
			return nil, errors.New("OCI registry allowlist entry is invalid")
		}
		if parsed.Scheme != "" && parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, errors.New("OCI registry allowlist supports only HTTP(S) transport")
		}
		host := strings.ToLower(parsed.Host)
		if parsed.Scheme == "http" {
			reader.insecureAllowed[host] = true
		}
		reader.allowed[host] = true
	}
	return reader, nil
}

func (reader *OCIImageMetadataReader) ReadImageMetadata(ctx context.Context, namespace, image, secretName string) (rolloutapp.ImageMetadataDTO, error) {
	if namespace == "" || secretName == "" {
		return rolloutapp.ImageMetadataDTO{}, errors.New("same-namespace image-pull Secret is required")
	}
	reference, err := name.NewDigest(image, name.WeakValidation)
	if err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI reference must be an immutable manifest digest")
	}
	registry := strings.ToLower(reference.Context().RegistryStr())
	if !reader.allowed[registry] {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI registry is not allowlisted")
	}
	digest := reference.DigestStr()
	secret := &corev1.Secret{}
	if err := reader.kube.Get(ctx, k8stypes.NamespacedName{Namespace: namespace, Name: secretName}, secret); err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("image-pull Secret is unavailable")
	}
	if secret.Type != corev1.SecretTypeDockerConfigJson {
		return rolloutapp.ImageMetadataDTO{}, errors.New("image-pull Secret has the wrong Kubernetes type")
	}
	configBytes, ok := secret.Data[corev1.DockerConfigJsonKey]
	if !ok || len(configBytes) == 0 {
		return rolloutapp.ImageMetadataDTO{}, errors.New("image-pull Secret has no Docker config JSON")
	}
	if err := validateDockerRegistryEntry(configBytes, registry); err != nil {
		return rolloutapp.ImageMetadataDTO{}, err
	}
	reader.mu.Lock()
	cached, found := reader.verified[digest]
	reader.mu.Unlock()
	if found {
		cached.Image = image
		return cached, nil
	}
	keychain, err := newStaticDockerKeychain(configBytes, registry)
	if err != nil {
		return rolloutapp.ImageMetadataDTO{}, err
	}
	if reader.insecureAllowed[registry] {
		reference, err = name.NewDigest(image, name.WeakValidation, name.Insecure)
		if err != nil {
			return rolloutapp.ImageMetadataDTO{}, errors.New("OCI reference is invalid")
		}
	}
	options := []remote.Option{remote.WithAuthFromKeychain(keychain), remote.WithContext(ctx)}
	descriptor, err := remote.Get(reference, options...)
	if err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI manifest or image config could not be fetched")
	}
	if descriptor.Digest.String() != digest {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI registry returned a different manifest digest")
	}
	if descriptor.MediaType != ocimediatypes.OCIManifestSchema1 && descriptor.MediaType != ocimediatypes.DockerManifestSchema2 {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI reference must resolve directly to a platform manifest, not an image index")
	}
	manifestBytes, err := descriptor.RawManifest()
	if err != nil || digestBytes(manifestBytes) != digest {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI manifest content hash does not match its immutable digest")
	}
	var manifest v1.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil || manifest.SchemaVersion != 2 || manifest.Config.Digest.String() == "" {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI manifest content is malformed")
	}
	imageObject, err := descriptor.Image()
	if err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI object is not a platform image manifest")
	}
	configDigest, err := imageObject.ConfigName()
	if err != nil || configDigest != manifest.Config.Digest {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI manifest config descriptor is inconsistent")
	}
	configBytes, err = imageObject.RawConfigFile()
	if err != nil || digestBytes(configBytes) != configDigest.String() {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI config content hash does not match its descriptor")
	}
	var config v1.ConfigFile
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI config content is malformed")
	}
	version := config.Config.Labels[versionLabel]
	features, err := membership.ParseBuildFeatureLabel(config.Config.Labels[featuresLabel])
	if err != nil {
		return rolloutapp.ImageMetadataDTO{}, errors.New("OCI config does not contain the canonical release feature label")
	}
	metadata := rolloutapp.ImageMetadataDTO{
		Image: image, ManifestDigest: digest, ConfigDigest: configDigest.String(), OS: config.OS, Architecture: config.Architecture,
		Version: version, Features: features,
	}
	reader.cache(digest, metadata)
	return metadata, nil
}

func (reader *OCIImageMetadataReader) cache(digest string, metadata rolloutapp.ImageMetadataDTO) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if _, exists := reader.verified[digest]; exists {
		return
	}
	if len(reader.order) == maxMetadataDigests {
		delete(reader.verified, reader.order[0])
		reader.order = reader.order[1:]
	}
	reader.verified[digest] = metadata
	reader.order = append(reader.order, digest)
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validateDockerRegistryEntry(value []byte, registry string) error {
	var config struct {
		Auths map[string]authn.AuthConfig `json:"auths"`
	}
	if err := json.Unmarshal(value, &config); err != nil || len(config.Auths) == 0 {
		return errors.New("image-pull Secret Docker config is malformed or has no direct credentials")
	}
	for host := range config.Auths {
		if normalizeRegistryKey(host) == registry {
			return nil
		}
	}
	return errors.New("image-pull Secret has no direct credentials for the target OCI registry")
}

func newStaticDockerKeychain(value []byte, registry string) (authn.Keychain, error) {
	var config struct {
		Auths map[string]authn.AuthConfig `json:"auths"`
	}
	if err := json.Unmarshal(value, &config); err != nil {
		return nil, errors.New("image-pull Secret Docker config is malformed")
	}
	if err := validateDockerRegistryEntry(value, registry); err != nil {
		return nil, err
	}
	auths := make(map[string]authn.AuthConfig, len(config.Auths))
	for host, auth := range config.Auths {
		auths[normalizeRegistryKey(host)] = auth
	}
	return staticDockerKeychain{auths: auths}, nil

}

type staticDockerKeychain struct {
	auths map[string]authn.AuthConfig
}

func (keychain staticDockerKeychain) Resolve(resource authn.Resource) (authn.Authenticator, error) {
	config, found := keychain.auths[strings.ToLower(resource.RegistryStr())]
	if !found {
		return nil, fmt.Errorf("no direct image-pull credentials for registry %s", resource.RegistryStr())
	}
	return authn.FromConfig(config), nil
}

func normalizeRegistryKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Hostname() != "" {
		value = parsed.Host
	}
	value = strings.TrimSuffix(value, "/v1/")
	value = strings.TrimSuffix(value, "/")
	return strings.ToLower(value)
}

var _ rolloutapp.ImageMetadataReader = (*OCIImageMetadataReader)(nil)
