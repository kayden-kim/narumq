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
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, nil, nil)
}

// NewRouterWithMembership constructs a router with membership and cluster-backup operations.
func NewRouterWithMembership(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	backupProvider ports.ClusterBackupProvider,
) http.Handler {
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, backupProvider)
}

func newRouter(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	backupProvider ports.ClusterBackupProvider,
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
	mux.HandleFunc("/health/live", auth.MaybeAllowProbe(healthH.Live))
	mux.HandleFunc("/health/ready", auth.MaybeAllowProbe(healthH.Ready))
	mux.HandleFunc("/health/startup", auth.MaybeAllowProbe(healthH.Startup))
	mux.HandleFunc("/health/drain", auth.MaybeAllowProbe(healthH.Drain))

	// Observability metrics
	mux.HandleFunc("/metrics", auth.MaybeAllowMetrics(metricsH.ServeHTTP))

	// Management API v1
	mux.HandleFunc("/api/v1/cluster", auth.RequireRole(RoleReadOnly, v1H.GetCluster))
	mux.HandleFunc("/api/v1/nodes", auth.RequireRole(RoleReadOnly, v1H.ListNodes))
	mux.HandleFunc("/api/v1/nodes/{nodeId}", auth.RequireRole(RoleReadOnly, v1H.GetNode))
	mux.HandleFunc("/api/v1/clients", auth.RequireRole(RoleReadOnly, v1H.ListClients))
	mux.HandleFunc("/api/v1/clients/{tenantId}/{clientId}", auth.RequireRole(RoleReadOnly, v1H.GetClient))
	mux.HandleFunc("/api/v1/subscriptions", auth.RequireRole(RoleReadOnly, v1H.ListSubscriptions))
	mux.HandleFunc("/api/v1/retained", auth.RequireRole(RoleReadOnly, v1H.ListRetained))
	mux.HandleFunc("/api/v1/tenants", auth.RequireRole(RoleClusterAdmin, v1H.ListTenants))
	if membershipService != nil {
		membershipH := &membershipHandler{service: membershipService}
		mux.HandleFunc("/api/v1/cluster/members", auth.RequireRole(RoleClusterAdmin, membershipH.StageMember))
		mux.HandleFunc("/api/v1/cluster/members/{memberId}", auth.RequireRole(RoleClusterAdmin, membershipH.RemoveMember))
		mux.HandleFunc("/api/v1/cluster/membership-operations/{operationId}", auth.RequireRole(RoleReadOnly, membershipH.GetOperation))
	}
	if backupProvider != nil {
		backupH := &clusterBackupHandler{provider: backupProvider}
		mux.HandleFunc("/api/v1/cluster/backups", auth.RequireRole(RoleClusterAdmin, backupH.CreateBackup))
	}

	// Unknown paths use the same error envelope as known routes.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, http.StatusNotFound, "resource_not_found", "resource not found", false, nil)
	})

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
	return newServer(cfg, stateProvider, healthChecker, metricsReg, nil, nil)
}

// NewServerWithMembership creates a management server with membership and cluster-backup routes.
func NewServerWithMembership(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	backupProvider ports.ClusterBackupProvider,
) *http.Server {
	return newServer(cfg, stateProvider, healthChecker, metricsReg, membershipService, backupProvider)
}

func newServer(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	backupProvider ports.ClusterBackupProvider,
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

	handler := newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, backupProvider)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  rTimeout,
		WriteTimeout: wTimeout,
	}
}
