package management

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/nodedrain"
)

// NodeDrainUseCase is the application boundary used by node drain routes.
type NodeDrainUseCase interface {
	StartNodeDrain(context.Context, membership.StartNodeDrainRequestDTO) (membership.NodeDrainOperationDTO, error)
	GetNodeDrain(context.Context, uint64) (membership.NodeDrainOperationDTO, error)
	CancelNodeDrain(context.Context, uint64) (membership.NodeDrainOperationDTO, error)
}

type nodeDrainHandler struct {
	service NodeDrainUseCase
}

type nodeDrainRequest struct {
	ServerReference                    *string `json:"serverReference"`
	ConnectionRatePerSecond            *uint32 `json:"connectionRatePerSecond"`
	LoadBalancerPropagationDelayMillis *uint64 `json:"loadBalancerPropagationDelayMillis"`
	TimeoutMillis                      *uint64 `json:"timeoutMillis"`
}

type nodeDrainOperationResponse struct {
	OperationID string               `json:"operationId"`
	NodeID      string               `json:"nodeId"`
	State       nodedrain.Lifecycle  `json:"state"`
	Phase       string               `json:"phase"`
	StartedAt   string               `json:"startedAt"`
	DeadlineAt  string               `json:"deadlineAt"`
	Connections nodeDrainConnections `json:"connections"`
	Sessions    nodeDrainSessions    `json:"sessions"`
	Replication nodeDrainReplication `json:"replication"`
	SafeToStop  bool                 `json:"safeToStop"`
	Failure     *nodeDrainFailure    `json:"failure"`
}

type nodeDrainConnections struct {
	Initial   uint64 `json:"initial"`
	Remaining uint64 `json:"remaining"`
}

type nodeDrainSessions struct {
	StateReadyForTakeover uint64 `json:"stateReadyForTakeover"`
	TakenOver             uint64 `json:"takenOver"`
	Failed                uint64 `json:"failed"`
}

type nodeDrainReplication struct {
	PendingWAL      uint64 `json:"pendingWal"`
	UnderReplicated uint64 `json:"underReplicated"`
}

type nodeDrainFailure struct {
	Code           string  `json:"code"`
	BlockingReason *string `json:"blockingReason"`
}

func (handler *nodeDrainHandler) Start(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	idempotencyKey, ok := idempotencyKeyFromRequest(w, r)
	if !ok {
		return
	}
	var request nodeDrainRequest
	if err := decodeStrictMembershipJSON(r, &request); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	rate := nodedrain.DefaultConnectionRatePerSecond
	if request.ConnectionRatePerSecond != nil {
		rate = *request.ConnectionRatePerSecond
	}
	lbDelayMillis := nodedrain.DefaultLoadBalancerPropagationDelayMillis
	if request.LoadBalancerPropagationDelayMillis != nil {
		lbDelayMillis = *request.LoadBalancerPropagationDelayMillis
	}
	timeoutMillis := nodedrain.DefaultTimeoutMillis
	if request.TimeoutMillis != nil {
		timeoutMillis = *request.TimeoutMillis
	}
	memberID, err := parseCanonicalMemberID(r.PathValue("nodeId"))
	if err != nil {
		writeNodeDrainNotFound(w, r)
		return
	}
	serverReference := ""
	if request.ServerReference != nil {
		serverReference = *request.ServerReference
	}
	operation, err := handler.service.StartNodeDrain(r.Context(), membership.StartNodeDrainRequestDTO{
		MemberID: memberID, IdempotencyKey: idempotencyKey, ServerReference: serverReference,
		ConnectionRatePerSecond:            rate,
		LoadBalancerPropagationDelayMillis: lbDelayMillis,
		TimeoutMillis:                      timeoutMillis,
	})
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if operation.Replayed {
		status = http.StatusOK
	}
	WriteDataEnvelope(w, r, status, nodeDrainOperationBody(operation))
}

func (handler *nodeDrainHandler) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	memberID, err := parseCanonicalMemberID(r.PathValue("nodeId"))
	if err != nil {
		writeNodeDrainNotFound(w, r)
		return
	}
	operation, err := handler.service.GetNodeDrain(r.Context(), memberID)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, nodeDrainOperationBody(operation))
}

func (handler *nodeDrainHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("DELETE node drain request must not contain a body"))
			return
		}
	}
	memberID, err := parseCanonicalMemberID(r.PathValue("nodeId"))
	if err != nil {
		writeNodeDrainNotFound(w, r)
		return
	}
	if _, ok := idempotencyKeyFromRequest(w, r); !ok {
		return
	}
	operation, err := handler.service.CancelNodeDrain(r.Context(), memberID)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, nodeDrainOperationBody(operation))
}

func writeNodeDrainNotFound(w http.ResponseWriter, r *http.Request) {
	WriteError(w, r, http.StatusNotFound, "resource_not_found", "node resource was not found", false, nil)
}

func nodeDrainOperationBody(operation membership.NodeDrainOperationDTO) nodeDrainOperationResponse {
	state := operation.State
	response := nodeDrainOperationResponse{
		OperationID: state.DrainOperationID,
		NodeID:      strconv.FormatUint(state.MemberID, 10),
		State:       nodedrain.LifecycleOf(state.Phase),
		Phase:       state.Phase.String(),
		StartedAt:   time.UnixMilli(int64(state.StartedAtUnixMillis)).UTC().Format(time.RFC3339Nano),
		DeadlineAt:  time.UnixMilli(int64(state.DeadlineAtUnixMillis)).UTC().Format(time.RFC3339Nano),
		Connections: nodeDrainConnections{
			Initial: state.Progress.ConnectionsInitial, Remaining: state.Progress.ConnectionsRemaining,
		},
		Sessions: nodeDrainSessions{
			StateReadyForTakeover: state.Progress.SessionsReadyForTakeover,
			TakenOver:             state.Progress.SessionsTakenOver, Failed: state.Progress.SessionsFailed,
		},
		Replication: nodeDrainReplication{
			PendingWAL: state.Progress.PendingWAL, UnderReplicated: state.Progress.UnderReplicated,
		},
		SafeToStop: state.Phase == nodedrain.PhaseCompleted,
	}
	if state.FailureCode != "" {
		failure := &nodeDrainFailure{Code: state.FailureCode}
		if state.BlockingReason != "" {
			failure.BlockingReason = &state.BlockingReason
		}
		response.Failure = failure
	}
	return response
}
