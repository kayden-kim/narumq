package management

import (
	"context"
	"net/http"
	"time"

	ports "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
)

// ServerConfig configures the HTTP management server.
type ServerConfig struct {
	ListenAddr            string
	AuthToken             string
	AuthTokenRole         Role
	APIKey                string
	APIKeyRole            Role
	AllowAnonymousProbes  bool
	AllowAnonymousMetrics bool
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
}

// NewRouter constructs the http.Handler for management and observability.
func NewRouter(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
) http.Handler {
	auth := NewAuthenticator(AuthConfig{
		AuthToken:             cfg.AuthToken,
		AuthTokenRole:         cfg.AuthTokenRole,
		APIKey:                cfg.APIKey,
		APIKeyRole:            cfg.APIKeyRole,
		AllowAnonymousProbes:  cfg.AllowAnonymousProbes,
		AllowAnonymousMetrics: cfg.AllowAnonymousMetrics,
	})

	healthH := NewHealthHandler(healthChecker)
	metricsH := NewMetricsHandler(metricsReg)
	v1H := NewV1Handler(stateProvider)

	mux := http.NewServeMux()

	// Health probes
	mux.HandleFunc("GET /health/live", auth.MaybeAllowProbe(healthH.Live))
	mux.HandleFunc("GET /health/ready", auth.MaybeAllowProbe(healthH.Ready))
	mux.HandleFunc("GET /health/startup", auth.MaybeAllowProbe(healthH.Startup))
	mux.HandleFunc("GET /health/drain", auth.MaybeAllowProbe(healthH.Drain))

	// Observability metrics
	mux.HandleFunc("GET /metrics", auth.MaybeAllowMetrics(metricsH.ServeHTTP))

	// Management API v1
	mux.HandleFunc("GET /api/v1/cluster", auth.RequireRole(RoleReadOnly, v1H.GetCluster))
	mux.HandleFunc("GET /api/v1/nodes", auth.RequireRole(RoleReadOnly, v1H.ListNodes))
	mux.HandleFunc("GET /api/v1/nodes/{nodeId}", auth.RequireRole(RoleReadOnly, v1H.GetNode))
	mux.HandleFunc("GET /api/v1/clients", auth.RequireRole(RoleReadOnly, v1H.ListClients))
	mux.HandleFunc("GET /api/v1/clients/{tenantId}/{clientId}", auth.RequireRole(RoleReadOnly, v1H.GetClient))
	mux.HandleFunc("GET /api/v1/subscriptions", auth.RequireRole(RoleReadOnly, v1H.ListSubscriptions))
	mux.HandleFunc("GET /api/v1/retained", auth.RequireRole(RoleReadOnly, v1H.ListRetained))
	mux.HandleFunc("GET /api/v1/tenants", auth.RequireRole(RoleClusterAdmin, v1H.ListTenants))

	// Global Request-ID middleware
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := GetOrGenerateRequestID(r)
		w.Header().Set(HeaderRequestID, reqID)
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// NewServer creates a configured *http.Server for management traffic.
func NewServer(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
) *http.Server {
	addr := cfg.ListenAddr
	if addr == "" {
		addr = ":8080"
	}
	rTimeout := cfg.ReadTimeout
	if rTimeout <= 0 {
		rTimeout = 10 * time.Second
	}
	wTimeout := cfg.WriteTimeout
	if wTimeout <= 0 {
		wTimeout = 10 * time.Second
	}

	handler := NewRouter(cfg, stateProvider, healthChecker, metricsReg)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  rTimeout,
		WriteTimeout: wTimeout,
	}
}
