package management

import (
	"errors"
	"net/http"
	"strconv"

	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

type clusterFeatureHandler struct {
	service MembershipUseCase
}

type clusterFeatureStateResponse struct {
	ActiveFeatures []string                       `json:"activeFeatures"`
	Members        []clusterFeatureMemberResponse `json:"members"`
}

type clusterFeatureMemberResponse struct {
	NodeID         string   `json:"nodeId"`
	ReleaseVersion string   `json:"releaseVersion"`
	Features       []string `json:"features"`
}

func (handler *clusterFeatureHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	state, err := handler.service.ClusterFeatures(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, clusterFeatureStateBody(state))
}

func (handler *clusterFeatureHandler) Activate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	feature := r.PathValue("feature")
	if !release.ValidFeature(feature) {
		writeInvalidRequest(w, r, errors.New("feature must be a valid feature token"))
		return
	}
	state, err := handler.service.ActivateFeature(r.Context(), feature)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, clusterFeatureStateBody(state))
}

func clusterFeatureStateBody(state membership.FeatureStateDTO) clusterFeatureStateResponse {
	activeFeatures := state.ActiveFeatures
	if activeFeatures == nil {
		activeFeatures = []string{}
	}
	members := make([]clusterFeatureMemberResponse, 0, len(state.Members))
	for _, member := range state.Members {
		features := member.Features
		if features == nil {
			features = []string{}
		}
		members = append(members, clusterFeatureMemberResponse{
			NodeID: strconv.FormatUint(member.MemberID, 10), ReleaseVersion: member.ReleaseVersion, Features: features,
		})
	}
	return clusterFeatureStateResponse{ActiveFeatures: activeFeatures, Members: members}
}
