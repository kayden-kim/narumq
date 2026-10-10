package management

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	ports "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/security"
)

// ServerConfig configures the HTTP management server.
type ServerConfig struct {
	ListenAddr            string
	AuthToken             string
	APIKeys               []APIKeyCredential
	OIDC                  *OIDCConfig
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
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

// NewRouterWithMembership constructs a router with cluster membership and administration routes.
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
	auditQueryService AuditQueryUseCase,
	credentialRevocationService CredentialRevocationUseCase,
	blocklistService BlocklistUseCase,
	clusterPolicyService ClusterPolicyUseCase,
) http.Handler {
	return newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService, auditQueryService, credentialRevocationService, blocklistService, clusterPolicyService)
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
	auditQueryService AuditQueryUseCase,
	credentialRevocationService CredentialRevocationUseCase,
	blocklistService BlocklistUseCase,
	clusterPolicyService ClusterPolicyUseCase,
) http.Handler {
	auth := NewAuthenticator(AuthConfig{
		AuthToken: cfg.AuthToken, APIKeys: cfg.APIKeys, OIDC: cfg.OIDC,
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
	mux.HandleFunc("/api/v1/cluster", auth.RequirePermission(PermissionRead, v1H.GetCluster))
	mux.HandleFunc("/api/v1/nodes", auth.RequirePermission(PermissionRead, v1H.ListNodes))
	mux.HandleFunc("/api/v1/nodes/{nodeId}", auth.RequirePermission(PermissionRead, v1H.GetNode))
	mux.HandleFunc("/api/v1/clients", auth.RequirePermission(PermissionRead, v1H.ListClients))
	mux.HandleFunc("/api/v1/clients/{tenantId}/{clientId}", auth.RequirePermission(PermissionRead, v1H.GetClient))
	mux.HandleFunc("/api/v1/subscriptions", auth.RequirePermission(PermissionRead, v1H.ListSubscriptions))
	mux.HandleFunc("/api/v1/retained", auth.RequirePermission(PermissionRead, v1H.ListRetained))
	mux.HandleFunc("/api/v1/tenants", auth.RequirePermission(PermissionClusterAdmin, v1H.ListTenants))
	methodNotAllowed := func(allow string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allow)
			WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		}
	}
	if auditQueryService != nil {
		queryHandler := &auditQueryHandler{service: auditQueryService, logger: cfg.Logger}
		mux.HandleFunc("/api/v1/audit/events", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				auth.RequirePermission(PermissionAuditRead, queryHandler.List)(w, r)
				return
			}
			auth.RequirePermission(PermissionRead, methodNotAllowed(http.MethodGet))(w, r)
		})
	}
	if credentialRevocationService != nil {
		credentialRevocationH := &credentialRevocationHandler{service: credentialRevocationService}
		mux.HandleFunc("/api/v1/security/credential-revocations", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				auth.RequirePermission(PermissionCredentialAdmin, credentialRevocationH.List)(w, r)
				return
			}
			auth.RequirePermission(PermissionCredentialAdmin, methodNotAllowed(http.MethodGet))(w, r)
		})
		mux.HandleFunc("/api/v1/security/credential-revocations/{credentialId}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationPermission(PermissionCredentialAdmin, auditMutation{
					action: "security.credential.revoke", targetType: "mqtt_credential", targetParam: "credentialId",
				}, credentialRevocationH.Revoke)(w, r)
			case http.MethodDelete:
				auth.RequireMutationPermission(PermissionCredentialAdmin, auditMutation{
					action: "security.credential.restore", targetType: "mqtt_credential", targetParam: "credentialId",
				}, credentialRevocationH.Restore)(w, r)
			default:
				auth.RequirePermission(PermissionCredentialAdmin, methodNotAllowed("POST, DELETE"))(w, r)
			}
		})
	}
	if blocklistService != nil {
		blocklistH := &blocklistHandler{service: blocklistService}
		mux.HandleFunc("/api/v1/security/blocklist", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				auth.RequirePermission(PermissionCredentialAdmin, blocklistH.List)(w, r)
				return
			}
			auth.RequirePermission(PermissionCredentialAdmin, methodNotAllowed(http.MethodGet))(w, r)
		})
		registerEntryRoute := func(pattern string, kind security.BlockKind) {
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					auth.RequireMutationPermission(PermissionCredentialAdmin, auditMutation{
						action: "security.blocklist.add", targetType: "mqtt_blocklist_entry",
					}, blocklistH.Block(kind))(w, r)
				case http.MethodDelete:
					auth.RequireMutationPermission(PermissionCredentialAdmin, auditMutation{
						action: "security.blocklist.remove", targetType: "mqtt_blocklist_entry",
					}, blocklistH.Unblock(kind))(w, r)
				default:
					auth.RequirePermission(PermissionCredentialAdmin, methodNotAllowed("POST, DELETE"))(w, r)
				}
			})
		}
		registerEntryRoute("/api/v1/security/blocklist/client-ids/{tenantId}/{clientId}", security.BlockKindClientID)
		registerEntryRoute("/api/v1/security/blocklist/usernames/{username}", security.BlockKindUsername)
		registerEntryRoute("/api/v1/security/blocklist/certificates/{certificateSha256}", security.BlockKindCertificateSHA256)
		registerEntryRoute("/api/v1/security/blocklist/ip-prefixes/{address}/{prefixLength}", security.BlockKindIPPrefix)
	}
	if membershipService != nil {
		membershipH := &membershipHandler{service: membershipService}
		mux.HandleFunc("/api/v1/cluster/members", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				auth.RequireMutationPermission(PermissionClusterAdmin, auditMutation{action: "cluster.member.stage", targetType: "cluster"}, membershipH.StageMember)(w, r)
				return
			}
			auth.RequirePermission(PermissionClusterAdmin, membershipH.StageMember)(w, r)
		})
		mux.HandleFunc("/api/v1/cluster/members/{memberId}", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				auth.RequireMutationPermission(PermissionClusterAdmin, auditMutation{action: "cluster.member.remove", targetType: "cluster_member", targetParam: "memberId"}, membershipH.RemoveMember)(w, r)
				return
			}
			auth.RequirePermission(PermissionClusterAdmin, membershipH.RemoveMember)(w, r)
		})
		mux.HandleFunc("/api/v1/cluster/membership-operations/{operationId}", auth.RequirePermission(PermissionRead, membershipH.GetOperation))
		featureH := &clusterFeatureHandler{service: membershipService}
		mux.HandleFunc("/api/v1/cluster/features", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				auth.RequirePermission(PermissionRead, featureH.List)(w, r)
			default:
				auth.RequirePermission(PermissionRead, methodNotAllowed(http.MethodGet))(w, r)
			}
		})
		mux.HandleFunc("/api/v1/cluster/features/{feature}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationPermission(PermissionClusterAdmin, auditMutation{action: "cluster.feature.activate", targetType: "cluster_feature", targetParam: "feature"}, featureH.Activate)(w, r)
			default:
				auth.RequirePermission(PermissionRead, methodNotAllowed(http.MethodPost))(w, r)
			}
		})
	}
	if clusterPolicyService != nil {
		policyH := &clusterPolicyHandler{service: clusterPolicyService}
		mux.HandleFunc("/api/v1/cluster/policy", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				auth.RequirePermission(PermissionRead, policyH.Get)(w, r)
			case http.MethodPut:
				auth.RequireMutationPermission(PermissionClusterAdmin, auditMutation{
					action: "cluster.policy.update", targetType: "cluster_policy",
				}, policyH.Update)(w, r)
			default:
				auth.RequirePermission(PermissionRead, methodNotAllowed("GET, PUT"))(w, r)
			}
		})
	}
	if drainService != nil {
		drainH := &nodeDrainHandler{service: drainService}
		mux.HandleFunc("/api/v1/nodes/{nodeId}/drain", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				auth.RequireMutationPermission(PermissionOperate, auditMutation{action: "node.drain.start", targetType: "node", targetParam: "nodeId"}, drainH.Start)(w, r)
			case http.MethodGet:
				auth.RequirePermission(PermissionRead, drainH.Get)(w, r)
			case http.MethodDelete:
				auth.RequireMutationPermission(PermissionOperate, auditMutation{action: "node.drain.cancel", targetType: "node", targetParam: "nodeId"}, drainH.Cancel)(w, r)
			default:
				auth.RequirePermission(PermissionRead, func(w http.ResponseWriter, r *http.Request) {
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
				auth.RequireMutationPermission(PermissionOperate, auditMutation{action: "cluster.rebalance.start", targetType: "cluster"}, rebalanceH.Start)(w, r)
			case http.MethodGet:
				auth.RequirePermission(PermissionRead, rebalanceH.Get)(w, r)
			case http.MethodDelete:
				auth.RequireMutationPermission(PermissionOperate, auditMutation{action: "cluster.rebalance.cancel", targetType: "cluster"}, rebalanceH.Cancel)(w, r)
			default:
				auth.RequirePermission(PermissionRead, func(w http.ResponseWriter, r *http.Request) {
					WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
				})(w, r)
			}
		})
	}
	if backupProvider != nil {
		backupH := &clusterBackupHandler{provider: backupProvider}
		mux.HandleFunc("/api/v1/cluster/backups", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				auth.RequireMutationPermission(PermissionClusterAdmin, auditMutation{action: "cluster.backup.create", targetType: "cluster"}, backupH.CreateBackup)(w, r)
				return
			}
			auth.RequirePermission(PermissionClusterAdmin, backupH.CreateBackup)(w, r)
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
	return newServer(cfg, stateProvider, healthChecker, metricsReg, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

// NewServerWithMembership creates a management server with cluster administration routes.
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
	auditQueryService AuditQueryUseCase,
	credentialRevocationService CredentialRevocationUseCase,
	blocklistService BlocklistUseCase,
	clusterPolicyService ClusterPolicyUseCase,
) *http.Server {
	return newServer(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService, auditQueryService, credentialRevocationService, blocklistService, clusterPolicyService)
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
	auditQueryService AuditQueryUseCase,
	credentialRevocationService CredentialRevocationUseCase,
	blocklistService BlocklistUseCase,
	clusterPolicyService ClusterPolicyUseCase,
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

	handler := newRouter(cfg, stateProvider, healthChecker, metricsReg, membershipService, drainService, rebalanceService, backupProvider, auditService, auditQueryService, credentialRevocationService, blocklistService, clusterPolicyService)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  rTimeout,
		WriteTimeout: wTimeout,
	}
}
