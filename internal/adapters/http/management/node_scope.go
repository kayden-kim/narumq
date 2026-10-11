package management

import (
	"net/http"
	"strconv"
)

func requireLocalNode(localNodeID uint64, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := parseCanonicalMemberID(r.PathValue("nodeId"))
		if err != nil {
			writeNodeDrainNotFound(w, r)
			return
		}
		if nodeID != localNodeID {
			WriteError(
				w, r, http.StatusConflict, "node_not_local",
				"nodeId does not identify the node serving this request", false,
				map[string]string{"servingNodeId": strconv.FormatUint(localNodeID, 10)},
			)
			return
		}
		next(w, r)
	}
}
