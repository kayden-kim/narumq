package membership

import (
	"errors"
	"slices"
	"strings"

	"narumq/internal/domain/release"
)

// BuildFeatures returns the canonical feature set advertised by a broker build.
func BuildFeatures(additional string) ([]string, error) {
	features := []string{
		ClientBlocklistFeature, ClusterPolicyFeature,
		ClusterPolicyTenantQuotasFeature, ConnectionRebalanceFeature,
		CredentialRevocationFeature, ProposalRecoveryFeature, SessionMetadataFeature,
		SharedSubscriptionRerouteFeature,
		release.ClusterObservabilityFeature,
	}
	if additional != "" {
		features = append(features, strings.Split(additional, ",")...)
	}
	slices.Sort(features)
	features = slices.Compact(features)
	if !release.ValidFeatureSet(features) {
		return nil, errors.New("build features are invalid")
	}
	return features, nil
}

// ParseBuildFeatureLabel validates the full, sorted feature set embedded in an OCI config.
func ParseBuildFeatureLabel(value string) ([]string, error) {
	if value == "" {
		return nil, errors.New("build feature label is empty")
	}
	features := strings.Split(value, ",")
	canonical, err := BuildFeatures(value)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(features, canonical) {
		return nil, errors.New("build feature label is not the canonical full feature set")
	}
	return features, nil
}
