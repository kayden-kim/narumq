package management

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
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
	RequestRate           int                    // Per-second rate and burst per authenticated principal; <= 0 uses the default.
	AuthFailureRate       int                    // Per-second rate and burst per remote IP; <= 0 uses the default.
	Clock                 applicationclock.Clock // Nil uses the system clock.
	AllowAnonymousProbes  bool
	AllowAnonymousMetrics bool
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	ClusterID             string
	Logger                *slog.Logger
}

// NewRouter constructs the http.Handler for management and observability.
func NewRouter(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
) http.Handler {
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, nil, nil, nil, nil, nil)
}

// NewRouterWithMembership constructs a router with membership, node drain, connection rebalance, and backup routes.
func NewRouterWithMembership(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	drainService NodeDrainUseCase,
	rebalanceService ConnectionRebalanceUseCase,
	backupProvider ports.ClusterBackupProvider,
	auditService AuditUseCase,
) http.Handler {
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService)
}

func newRouter(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	drainService NodeDrainUseCase,
	rebalanceService ConnectionRebalanceUseCase,
	backupProvider ports.ClusterBackupProvider,
	auditService AuditUseCase,
) http.Handler {
	auth := NewAuthenticator(AuthConfig{
		AuthToken: cfg.AuthToken, AuthTokenRole: cfg.AuthTokenRole,
		APIKey: cfg.APIKey, APIKeyRole: cfg.APIKeyRole,
		RequestRate: cfg.RequestRate, AuthFailureRate: cfg.AuthFailureRate, Clock: cfg.Clock,
		AllowAnonymousProbes: cfg.AllowAnonymousProbes, AllowAnonymousMetrics: cfg.AllowAnonymousMetrics,
		Audit: auditService, Logger: cfg.Logger, ClusterID: cfg.ClusterID,
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
		mux.HandleFunc("/api/v1/cluster/members", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				auth.RequireMutationRole(RoleClusterAdmin, auditMutation{action: "cluster.member.stage", targetType: "cluster"}, membershipH.StageMember)(w, r)
				return
			}
			auth.RequireRole(RoleClusterAdmin, membershipH.StageMember)(w, r)
		})
		mux.HandleFunc("/api/v1/cluster/members/{memberId}", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				auth.RequireMutationRole(RoleClusterAdmin, auditMutation{action: "cluster.member.remove", targetType: "cluster_member", targetParam: "memberId"}, membershipH.RemoveMember)(w, r)
				return
			}
			auth.RequireRole(RoleClusterAdmin, membershipH.RemoveMember)(w, r)
		})
		mux.HandleFunc("/api/v1/cluster/membership-operations/{operationId}", auth.RequireRole(RoleReadOnly, membershipH.GetOperation))
		featureH := &clusterFeatureHandler{service: membershipService}
		methodNotAllowed := func(allow string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Allow", allow)
				WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
			}
		}
		mux.HandleFunc("/api/v1/cluster/features", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				auth.RequireRole(RoleReadOnly, featureH.List)(w, r)
			default:
				auth.RequireRole(RoleReadOnly, methodNotAllowed(http.MethodGet))(w, r)
			}
		})
		mux.HandleFunc("/api/v1/cluster/features/{feature}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationRole(RoleClusterAdmin, auditMutation{action: "cluster.feature.activate", targetType: "cluster_feature", targetParam: "feature"}, featureH.Activate)(w, r)
			default:
				auth.RequireRole(RoleReadOnly, methodNotAllowed(http.MethodPost))(w, r)
			}
		})
	}
	if drainService != nil {
		drainH := &nodeDrainHandler{service: drainService}
		mux.HandleFunc("/api/v1/nodes/{nodeId}/drain", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationRole(RoleOperator, auditMutation{action: "node.drain.start", targetType: "node", targetParam: "nodeId"}, drainH.Start)(w, r)
			case http.MethodGet:
				auth.RequireRole(RoleReadOnly, drainH.Get)(w, r)
			case http.MethodDelete:
				auth.RequireMutationRole(RoleOperator, auditMutation{action: "node.drain.cancel", targetType: "node", targetParam: "nodeId"}, drainH.Cancel)(w, r)
			default:
				auth.RequireRole(RoleReadOnly, func(w http.ResponseWriter, r *http.Request) {
					WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
				})(w, r)
			}
		})
	}
	if rebalanceService != nil {
		rebalanceH := &connectionRebalanceHandler{service: rebalanceService}
		mux.HandleFunc("/api/v1/cluster/rebalance", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationRole(RoleOperator, auditMutation{action: "cluster.rebalance.start", targetType: "cluster"}, rebalanceH.Start)(w, r)
			case http.MethodGet:
				auth.RequireRole(RoleReadOnly, rebalanceH.Get)(w, r)
			case http.MethodDelete:
				auth.RequireMutationRole(RoleOperator, auditMutation{action: "cluster.rebalance.cancel", targetType: "cluster"}, rebalanceH.Cancel)(w, r)
			default:
				auth.RequireRole(RoleReadOnly, func(w http.ResponseWriter, r *http.Request) {
					WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
				})(w, r)
			}
		})
	}
	if backupProvider != nil {
		backupH := &clusterBackupHandler{provider: backupProvider}
		mux.HandleFunc("/api/v1/cluster/backups", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				auth.RequireMutationRole(RoleClusterAdmin, auditMutation{action: "cluster.backup.create", targetType: "cluster"}, backupH.CreateBackup)(w, r)
				return
			}
			auth.RequireRole(RoleClusterAdmin, backupH.CreateBackup)(w, r)
		})
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
	return newServer(cfg, stateProvider, healthChecker, metricsReg, nil, nil, nil, nil, nil)
}

// NewServerWithMembership creates a management server with membership, node drain, connection rebalance, and backup routes.
func NewServerWithMembership(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	drainService NodeDrainUseCase,
	rebalanceService ConnectionRebalanceUseCase,
	backupProvider ports.ClusterBackupProvider,
	auditService AuditUseCase,
) *http.Server {
	return newServer(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService)
}

func newServer(
	cfg ServerConfig,
	stateProvider ports.StateProvider,
	healthChecker ports.HealthChecker,
	metricsReg portsmetrics.MetricsRenderer,
	membershipService MembershipUseCase,
	drainService NodeDrainUseCase,
	rebalanceService ConnectionRebalanceUseCase,
	backupProvider ports.ClusterBackupProvider,
	auditService AuditUseCase,
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

	handler := newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  rTimeout,
		WriteTimeout: wTimeout,
	}
}
