package management

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"

	ports "narumq/internal/application/ports/management"
)

type clusterBackupHandler struct {
	provider ports.ClusterBackupProvider
}

func (handler *clusterBackupHandler) CreateBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if requestHasBody(r) {
		WriteError(w, r, http.StatusBadRequest, "request_body_not_allowed", "request body must be empty", false, nil)
		return
	}

	backup, err := handler.provider.CaptureClusterBackup(r.Context())
	if err != nil {
		if errors.Is(err, ports.ErrClusterBackupUnavailable) {
			WriteError(w, r, http.StatusServiceUnavailable, "cluster_backup_unavailable", "cluster backup is temporarily unavailable", true, nil)
			return
		}
		WriteError(w, r, http.StatusInternalServerError, "internal_error", "internal cluster backup error", false, nil)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\"narumq-backup-"+strconv.FormatUint(backup.CommitIndex, 10)+".nmqbak\"")
	w.Header().Set("X-NaruMQ-Backup-Commit-Index", strconv.FormatUint(backup.CommitIndex, 10))
	w.Header().Set("X-NaruMQ-Backup-SHA256", hex.EncodeToString(backup.ArchiveSHA256[:]))
	w.Header().Set("Content-Length", strconv.Itoa(len(backup.Archive)))
	w.Header().Set(HeaderRequestID, GetOrGenerateRequestID(r))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(backup.Archive)
}

func requestHasBody(r *http.Request) bool {
	if r.ContentLength > 0 {
		return true
	}
	if r.ContentLength == 0 && len(r.TransferEncoding) == 0 {
		return false
	}
	if r.Body == nil {
		return false
	}
	var probe [1]byte
	n, err := io.ReadFull(r.Body, probe[:])
	return n != 0 || !errors.Is(err, io.EOF)
}
