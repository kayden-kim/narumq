package management

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/connrebalance"
)

// ConnectionRebalanceUseCase is the application boundary for cluster rebalance routes.
type ConnectionRebalanceUseCase interface {
	StartConnectionRebalance(context.Context, membership.StartConnectionRebalanceRequestDTO) (membership.ConnectionRebalanceOperationDTO, error)
	GetConnectionRebalance(context.Context) (membership.ConnectionRebalanceOperationDTO, error)
	CancelConnectionRebalance(context.Context) (membership.ConnectionRebalanceOperationDTO, error)
}

type connectionRebalanceHandler struct {
	service ConnectionRebalanceUseCase
}

type connectionRebalanceRequest struct {
	ServerReference                    *string `json:"serverReference"`
	MaxDeviationConnections            *uint64 `json:"maxDeviationConnections"`
	ConnectionRatePerSecond            *uint32 `json:"connectionRatePerSecond"`
	LoadBalancerPropagationDelayMillis *uint64 `json:"loadBalancerPropagationDelayMillis"`
	TimeoutMillis                      *uint64 `json:"timeoutMillis"`
}

type connectionRebalanceResponse struct {
	OperationID                        string                      `json:"operationId"`
	State                              string                      `json:"state"`
	ServerReference                    *string                     `json:"serverReference"`
	MaxDeviationConnections            uint64                      `json:"maxDeviationConnections"`
	ConnectionRatePerSecond            uint32                      `json:"connectionRatePerSecond"`
	LoadBalancerPropagationDelayMillis uint64                      `json:"loadBalancerPropagationDelayMillis"`
	TimeoutMillis                      uint64                      `json:"timeoutMillis"`
	StartedAt                          *string                     `json:"startedAt"`
	DeadlineAt                         *string                     `json:"deadlineAt"`
	FinishedAt                         *string                     `json:"finishedAt"`
	TotalConnections                   uint64                      `json:"totalConnections"`
	FairShare                          uint64                      `json:"fairShare"`
	Ceiling                            uint64                      `json:"ceiling"`
	Nodes                              []connectionRebalanceNode   `json:"nodes"`
	Failure                            *connectionRebalanceFailure `json:"failure"`
}

type connectionRebalanceNode struct {
	NodeID              string  `json:"nodeId"`
	Connections         uint64  `json:"connections"`
	SessionsTransferred uint64  `json:"sessionsTransferred"`
	Shedding            bool    `json:"shedding"`
	ReportedAt          *string `json:"reportedAt"`
}

type connectionRebalanceFailure struct {
	Code string `json:"code"`
}

func (handler *connectionRebalanceHandler) Start(w http.ResponseWriter, r *http.Request) {
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
	var request connectionRebalanceRequest
	if err := decodeStrictMembershipJSON(r, &request); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if request.MaxDeviationConnections == nil || request.ConnectionRatePerSecond == nil || request.LoadBalancerPropagationDelayMillis == nil || request.TimeoutMillis == nil {
		writeInvalidRequest(w, r, errors.New("maxDeviationConnections, connectionRatePerSecond, loadBalancerPropagationDelayMillis, and timeoutMillis are required"))
		return
	}
	serverReference := ""
	if request.ServerReference != nil {
		serverReference = *request.ServerReference
	}
	operation, err := handler.service.StartConnectionRebalance(r.Context(), membership.StartConnectionRebalanceRequestDTO{
		IdempotencyKey: idempotencyKey, ServerReference: serverReference,
		MaxDeviationConnections: *request.MaxDeviationConnections, ConnectionRatePerSecond: *request.ConnectionRatePerSecond,
		LoadBalancerPropagationDelayMillis: *request.LoadBalancerPropagationDelayMillis, TimeoutMillis: *request.TimeoutMillis,
	})
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if operation.Replayed {
		status = http.StatusOK
	}
	WriteDataEnvelope(w, r, status, connectionRebalanceOperationBody(operation))
}

func (handler *connectionRebalanceHandler) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	operation, err := handler.service.GetConnectionRebalance(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, connectionRebalanceOperationBody(operation))
}

func (handler *connectionRebalanceHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if r.Body != nil {
		if body, err := readEmptyRequestBody(r); err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("DELETE connection rebalance request must not contain a body"))
			return
		}
	}
	if _, ok := idempotencyKeyFromRequest(w, r); !ok {
		return
	}
	operation, err := handler.service.CancelConnectionRebalance(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, connectionRebalanceOperationBody(operation))
}

func readEmptyRequestBody(r *http.Request) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, 1))
}

func connectionRebalanceOperationBody(operation membership.ConnectionRebalanceOperationDTO) connectionRebalanceResponse {
	state := operation.State
	reports := make([]connrebalance.Report, 0, len(state.Participants))
	nodes := make([]connectionRebalanceNode, 0, len(state.Participants))
	for _, participant := range state.Participants {
		reports = append(reports, connrebalance.Report{NodeID: participant.NodeID, Connections: participant.Connections, Shedding: participant.Shedding, ReportedAtUnixMillis: participant.ReportedAtUnixMillis})
		nodes = append(nodes, connectionRebalanceNode{
			NodeID: strconv.FormatUint(participant.NodeID, 10), Connections: participant.Connections,
			SessionsTransferred: participant.SessionsTransferred, Shedding: participant.Shedding,
			ReportedAt: rebalanceTime(participant.ReportedAtUnixMillis),
		})
	}
	evaluation := connrebalance.Evaluate(reports, state.MaxDeviationConnections, 0)
	serverReference := (*string)(nil)
	if state.ServerReference != "" {
		value := state.ServerReference
		serverReference = &value
	}
	response := connectionRebalanceResponse{
		OperationID: state.RebalanceOperationID, State: state.Phase.String(), ServerReference: serverReference,
		MaxDeviationConnections: state.MaxDeviationConnections, ConnectionRatePerSecond: state.ConnectionRatePerSecond,
		LoadBalancerPropagationDelayMillis: state.LoadBalancerPropagationDelayMillis, TimeoutMillis: state.TimeoutMillis,
		StartedAt: rebalanceTime(state.StartedAtUnixMillis), DeadlineAt: rebalanceTime(state.DeadlineAtUnixMillis),
		FinishedAt: rebalanceTime(state.FinishedAtUnixMillis), TotalConnections: evaluation.Total,
		FairShare: evaluation.FairShare, Ceiling: evaluation.Ceiling, Nodes: nodes,
	}
	if state.FailureCode != "" {
		response.Failure = &connectionRebalanceFailure{Code: state.FailureCode}
	}
	return response
}

func rebalanceTime(milliseconds uint64) *string {
	if milliseconds == 0 {
		return nil
	}
	value := time.UnixMilli(int64(milliseconds)).UTC().Format(time.RFC3339Nano)
	return &value
}
