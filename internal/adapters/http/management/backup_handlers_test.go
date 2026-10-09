package management

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	adaptersmetrics "narumq/internal/adapters/metrics"
	ports "narumq/internal/application/ports/management"
)

type clusterBackupProviderStub struct {
	backup ports.ClusterBackupDTO
	err    error
	calls  int
}

func (stub *clusterBackupProviderStub) CaptureClusterBackup(context.Context) (ports.ClusterBackupDTO, error) {
	stub.calls++
	return stub.backup, stub.err
}

func newBackupHTTPTestHandler(provider ports.ClusterBackupProvider, role Role) http.Handler {
	return newRouter(
		ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "cluster-secret", Roles: []Role{role}}}, ClusterID: "test-cluster"},
		&mockStateProvider{}, nil, nil, nil, nil, nil, provider, &spyAuditUseCase{}, nil, nil,
	)
}

func TestClusterBackupHTTPStreamsArchiveAndBoundaryHeaders(t *testing.T) {
	framed := []byte("NMQBKUP2\x00fixture")
	checksum := sha256.Sum256(framed)
	archive := append(append([]byte(nil), framed...), checksum[:]...)
	provider := &clusterBackupProviderStub{backup: ports.ClusterBackupDTO{
		CommitIndex: 42, ArchiveSHA256: checksum, Archive: archive,
	}}
	handler := newBackupHTTPTestHandler(provider, RoleClusterAdmin)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	request.Header.Set("X-API-Key", "cluster-secret")
	request.Header.Set(HeaderRequestID, "backup-request-42")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("content type=%q", got)
	}
	if got := response.Header().Get("Content-Disposition"); got != `attachment; filename="narumq-backup-42.nmqbak"` {
		t.Fatalf("content disposition=%q", got)
	}
	if got := response.Header().Get("X-Narumq-Backup-Commit-Index"); got != "42" {
		t.Fatalf("commit index header=%q", got)
	}
	if got := response.Header().Get("X-Narumq-Backup-SHA256"); got != "4fad4467470fe92dd25a91fea286ec44502af592942608b5257bfcd97e9617af" {
		t.Fatalf("archive checksum header=%q", got)
	}
	if got := response.Header().Get("Content-Length"); got != strconv.Itoa(len(archive)) {
		t.Fatalf("content length=%q, want %d", got, len(archive))
	}
	if got := response.Header().Get(HeaderRequestID); got != "backup-request-42" {
		t.Fatalf("request ID=%q", got)
	}
	if !bytes.Equal(response.Body.Bytes(), archive) {
		t.Fatalf("archive body=%q, want %q", response.Body.Bytes(), archive)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.calls)
	}
}

func TestClusterBackupHTTPRequiresClusterAdmin(t *testing.T) {
	provider := &clusterBackupProviderStub{}
	handler := newBackupHTTPTestHandler(provider, RoleReadOnly)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	request.Header.Set("X-API-Key", "cluster-secret")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if provider.calls != 0 {
		t.Fatalf("provider called %d times without ClusterAdmin", provider.calls)
	}
}

// Traceability: UI-01; docs/specs/management-api-and-observability.md §8.
func TestReadOnlyAPIKeyCanReadButCannotCreateBackup(t *testing.T) {
	provider := &clusterBackupProviderStub{backup: ports.ClusterBackupDTO{CommitIndex: 42}}
	handler := NewRouterWithMembership(
		ServerConfig{
			AuthToken: "admin-secret",
			APIKeys:   []APIKeyCredential{{ID: "dashboard", Key: "read-only-secret", Roles: []Role{RoleReadOnly}}},
			ClusterID: "test-cluster",
		},
		&mockStateProvider{}, nil, adaptersmetrics.NewRegistry(), nil, nil, nil, provider, &spyAuditUseCase{}, nil, nil,
	)

	for _, path := range []string{"/metrics", "/api/v1/cluster"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("X-API-Key", "read-only-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("read-only GET %s status = %d, want 200; body = %s", path, response.Code, response.Body.String())
		}
	}

	readOnlyBackup := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	readOnlyBackup.Header.Set("X-API-Key", "read-only-secret")
	readOnlyResponse := httptest.NewRecorder()
	handler.ServeHTTP(readOnlyResponse, readOnlyBackup)
	if readOnlyResponse.Code != http.StatusForbidden || provider.calls != 0 {
		t.Fatalf("read-only backup status = %d, provider calls = %d; want 403 and no capture",
			readOnlyResponse.Code, provider.calls)
	}

	adminBackup := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	adminBackup.Header.Set("Authorization", "Bearer admin-secret")
	adminResponse := httptest.NewRecorder()
	handler.ServeHTTP(adminResponse, adminBackup)
	if adminResponse.Code != http.StatusOK || provider.calls != 1 {
		t.Fatalf("admin backup status = %d, provider calls = %d; want 200 and one capture",
			adminResponse.Code, provider.calls)
	}
}

func TestClusterBackupHTTPRejectsWrongMethodAndRequestBody(t *testing.T) {
	provider := &clusterBackupProviderStub{}
	handler := newBackupHTTPTestHandler(provider, RoleClusterAdmin)
	chunkedBody := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", strings.NewReader("unexpected"))
	chunkedBody.ContentLength = -1
	chunkedBody.TransferEncoding = []string{"chunked"}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/v1/cluster/backups", nil),
		httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", strings.NewReader("unexpected")),
		chunkedBody,
	} {
		request.Header.Set("X-API-Key", "cluster-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		wantStatus := http.StatusMethodNotAllowed
		if request.Method == http.MethodPost {
			wantStatus = http.StatusBadRequest
		}
		if response.Code != wantStatus {
			t.Fatalf("%s status=%d want=%d body=%s", request.Method, response.Code, wantStatus, response.Body.String())
		}
	}
	if provider.calls != 0 {
		t.Fatalf("provider called %d times for invalid requests", provider.calls)
	}
}

func TestClusterBackupHTTPAcceptsEmptyChunkedRequest(t *testing.T) {
	provider := &clusterBackupProviderStub{}
	handler := newBackupHTTPTestHandler(provider, RoleClusterAdmin)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", strings.NewReader(""))
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	request.Header.Set("X-API-Key", "cluster-secret")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.calls)
	}
}

func TestClusterBackupHTTPReportsUnavailableAndInternalErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantRetry  bool
	}{
		{name: "quorum unavailable", err: ports.ErrClusterBackupUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: "cluster_backup_unavailable", wantRetry: true},
		{name: "unexpected failure", err: errors.New("disk details stay private"), wantStatus: http.StatusInternalServerError, wantCode: "internal_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newBackupHTTPTestHandler(&clusterBackupProviderStub{err: test.err}, RoleClusterAdmin)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
			request.Header.Set("X-API-Key", "cluster-secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			var body struct {
				Error struct {
					Code      string `json:"code"`
					Retryable bool   `json:"retryable"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Error.Code != test.wantCode || body.Error.Retryable != test.wantRetry {
				t.Fatalf("error=%+v want code=%q retryable=%t", body.Error, test.wantCode, test.wantRetry)
			}
			if strings.Contains(response.Body.String(), "disk details stay private") {
				t.Fatal("response leaked internal error details")
			}
		})
	}
}
func TestClusterBackupHTTPAcceptsUnknownLengthWithoutBodyReader(t *testing.T) {
	provider := &clusterBackupProviderStub{}
	handler := newBackupHTTPTestHandler(provider, RoleClusterAdmin)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	request.Body = nil
	request.Header.Set("X-API-Key", "cluster-secret")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || provider.calls != 1 {
		t.Fatalf("status=%d provider calls=%d body=%s, want empty request accepted once", response.Code, provider.calls, response.Body.String())
	}
}
