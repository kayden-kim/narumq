package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJWKSHandlerServesOnlyGETJWKS(t *testing.T) {
	const content = `{"keys":[]}`
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := jwksHandler(path)

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/jwks.json", nil))
	if get.Code != http.StatusOK || get.Body.String() != content {
		t.Fatalf("GET /jwks.json = %d %q, want 200 and exact file bytes", get.Code, get.Body.String())
	}

	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/jwks.json", nil))
	if post.Code != http.StatusMethodNotAllowed || post.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST /jwks.json = %d allow=%q, want 405 Allow: GET", post.Code, post.Header().Get("Allow"))
	}

	other := httptest.NewRecorder()
	handler.ServeHTTP(other, httptest.NewRequest(http.MethodGet, "/private", nil))
	if other.Code != http.StatusNotFound {
		t.Fatalf("GET /private = %d, want 404", other.Code)
	}
}

func TestServeJWKSCommandRequiresFileAndListen(t *testing.T) {
	var stderr bytes.Buffer
	err := serveJWKSCommand(nil, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--file and --listen") {
		t.Fatalf("serveJWKSCommand() error = %v, want required-flag error", err)
	}
}

func TestRunCLIRequiresSubcommand(t *testing.T) {
	err := runCLI(nil, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "subcommand is required") {
		t.Fatalf("runCLI() error = %v, want missing-subcommand error", err)
	}
}
