package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
	paho5 "github.com/eclipse/paho.golang/paho"
)

type drainProfile struct {
	profile
	Cluster struct {
		CommonEndpoint                     string `json:"commonEndpoint"`
		ServerReference                    string `json:"serverReference"`
		ObserverEndpoint                   string `json:"observerEndpoint"`
		DrainNodeID                        int    `json:"drainNodeId"`
		ConnectionRatePerSecond            int    `json:"connectionRatePerSecond"`
		LoadBalancerPropagationDelayMillis int    `json:"loadBalancerPropagationDelayMillis"`
		DrainTimeoutMillis                 int    `json:"drainTimeoutMillis"`
	} `json:"cluster"`
	FakeServer struct {
		Endpoint     string `json:"endpoint"`
		TrapEndpoint string `json:"trapEndpoint"`
	} `json:"fakeServer"`
	ClientIDs struct {
		Client    string `json:"client"`
		Observer  string `json:"observer"`
		Publisher string `json:"publisher"`
	} `json:"clientIds"`
	ExpectedCases []drainCase `json:"expectedCases"`
}
type drainCase struct {
	CaseID          string        `json:"caseId"`
	Protocol        string        `json:"protocol"`
	Profile         string        `json:"profile"`
	Kind            string        `json:"kind"`
	Vector          string        `json:"vector"`
	ExpectedResults []profileRole `json:"expectedResults"`
}
type drainCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func drainDecode(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("drain input contains trailing data")
	}
	return nil
}
func loadDrainProfile(path string) (drainProfile, error) {
	var p drainProfile
	if err := drainDecode(path, &p); err != nil {
		return p, err
	}
	if p.SchemaVersion != 1 || p.ConnectTimeoutMillis != 10000 || p.OperationTimeoutMillis != 45000 || p.SessionExpirySeconds != 60 || p.WillDelaySeconds != 5 || p.ReconnectWithinMillis != 2000 || p.ReconnectBaseMillis != 100 || p.ReconnectCapMillis != 2000 || p.MaxReconnectAttempts != 5 || p.JitterSeed != 20260921 {
		return p, errors.New("invalid drain timing profile")
	}
	return p, nil
}

func validServerReference(ref string) bool {
	if strings.ContainsAny(ref, ",/?#@[]") || strings.IndexFunc(ref, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == '\v' || r == '\f' || r > 127
	}) >= 0 {
		return false
	}
	index := strings.LastIndexByte(ref, ':')
	if index <= 0 {
		return false
	}
	host, port := ref[:index], ref[index+1:]
	if port == "" {
		return false
	}
	for _, ch := range port {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || len(host) > 253 || net.ParseIP(host) != nil {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}

type drainDecision struct {
	surface, reference, sdkError string
	reason                       byte
	accepted                     bool
	at                           time.Time
}
type drainConnack struct {
	endpoint string
	present  bool
	at       time.Time
}
type drainSafeClient struct {
	manager       *autopaho.ConnectionManager
	up            chan drainConnack
	decisions     chan drainDecision
	disconnected  chan drainDecision
	messages      chan mqtt5Message
	once          sync.Once
	mu            sync.Mutex
	connectedOnce bool
	lastDial      string
	attempts      int
	cancel        context.CancelFunc
}

func newDrainSafeClient(ctx context.Context, address, clientID string, p drainProfile, credentials drainCredentials, willTopic string, willPayload []byte) (*drainSafeClient, error) {
	connectionCtx, cancel := context.WithCancel(ctx)
	client := &drainSafeClient{up: make(chan drainConnack, 8), decisions: make(chan drainDecision, 1), disconnected: make(chan drainDecision, 1), messages: make(chan mqtt5Message, 16), cancel: cancel}
	classify := func(d drainDecision) {
		client.once.Do(func() {
			d.at = time.Now()
			select {
			case client.decisions <- d:
			default:
			}
			select {
			case client.disconnected <- d:
			default:
			}
		})
	}
	endpoint, err := url.Parse("mqtt://" + address)
	if err != nil {
		cancel()
		return nil, err
	}
	jitter := newJitter(p.profile)
	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{endpoint}, KeepAlive: 5, CleanStartOnInitialConnection: true, SessionExpiryInterval: uint32(p.SessionExpirySeconds), ConnectTimeout: p.connectTimeout(), ConnectUsername: credentials.Username, ConnectPassword: []byte(credentials.Password),
		ReconnectBackoff: func(attempt int) time.Duration {
			return time.Duration(jitter(attempt).ChosenDelayMillis) * time.Millisecond
		},
		ConnectPacketBuilder: func(connect *paho5.Connect, _ *url.URL) (*paho5.Connect, error) {
			if connect.Properties == nil {
				connect.Properties = &paho5.ConnectProperties{}
			}
			// True is the MQTT default, so Paho omits property 0x17.
			connect.Properties.RequestProblemInfo = true
			return connect, nil
		},
		AttemptConnection: func(attemptCtx context.Context, _ autopaho.ClientConfig, serverURL *url.URL) (net.Conn, error) {
			client.mu.Lock()
			connected := client.connectedOnce
			attempt := client.attempts
			if connected {
				client.attempts++
			}
			client.mu.Unlock()
			if connected && attempt >= p.MaxReconnectAttempts {
				cancel()
				return nil, errors.New("maximum reconnect attempts exceeded")
			}
			target := serverURL.Host
			if connected && attempt == 0 {
				timer := time.NewTimer(time.Second)
				select {
				case d := <-client.decisions:
					if d.accepted {
						target = d.reference
					}
				case <-timer.C:
				case <-attemptCtx.Done():
					timer.Stop()
					return nil, attemptCtx.Err()
				}
				timer.Stop()
			}
			dial := func(endpoint string) (net.Conn, error) {
				client.mu.Lock()
				client.lastDial = endpoint
				client.mu.Unlock()
				return (&net.Dialer{Timeout: p.connectTimeout()}).DialContext(attemptCtx, "tcp", endpoint)
			}
			conn, err := dial(target)
			if err != nil && target != serverURL.Host {
				conn, err = dial(serverURL.Host)
			}
			if err != nil {
				return nil, err
			}
			return packets.NewThreadSafeConn(conn), nil
		},
		OnConnectionUp: func(_ *autopaho.ConnectionManager, ack *paho5.Connack) {
			client.mu.Lock()
			client.connectedOnce = true
			target := client.lastDial
			client.mu.Unlock()
			select {
			case client.up <- drainConnack{endpoint: target, present: ack.SessionPresent, at: time.Now()}:
			default:
			}
		},
		OnConnectionDown: func() bool { return true },
		ClientConfig: paho5.ClientConfig{
			ClientID: clientID,
			OnServerDisconnect: func(d *paho5.Disconnect) {
				ref := ""
				if d.Properties != nil {
					ref = d.Properties.ServerReference
				}
				classify(drainDecision{surface: "server_disconnect", reason: d.ReasonCode, reference: ref, accepted: d.ReasonCode == 0x9c && validServerReference(ref)})
			},
			OnClientError: func(err error) { classify(drainDecision{surface: "client_error", sdkError: err.Error()}) },
			OnPublishReceived: []func(paho5.PublishReceived) (bool, error){func(received paho5.PublishReceived) (bool, error) {
				if received.Packet != nil {
					select {
					case client.messages <- mqtt5Message{topic: received.Packet.Topic, payload: append([]byte(nil), received.Packet.Payload...)}:
					default:
					}
				}
				return true, nil
			}},
		},
	}
	if willTopic != "" {
		delay := uint32(p.WillDelaySeconds)
		cfg.WillMessage = &paho5.WillMessage{Topic: willTopic, Payload: willPayload, QoS: 0}
		cfg.WillProperties = &paho5.WillProperties{WillDelayInterval: &delay}
	}
	manager, err := autopaho.NewConnection(connectionCtx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	client.manager = manager
	return client, nil
}
func (client *drainSafeClient) await(ctx context.Context) (drainConnack, error) {
	select {
	case ack := <-client.up:
		return ack, nil
	case <-ctx.Done():
		return drainConnack{}, ctx.Err()
	case <-client.manager.Done():
		return drainConnack{}, errors.New("connection manager stopped before CONNACK")
	}
}
func (client *drainSafeClient) close() {
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = client.manager.Disconnect(closeCtx)
	client.cancel()
}

func runDrainDriver(options driverOptions) error {
	if err := validateOutput(options.output); err != nil {
		return err
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	value := result{SchemaVersion: 1, CaseID: options.caseID, SDK: "go", SDKVersion: go5Version, Protocol: "mqtt5", Profile: "MQTT5_DRAIN_SAFE", Role: options.role, Status: "failed", StartedAt: started, FinishedAt: started, Observations: map[string]any{}}
	p, runErr := loadDrainProfile(options.profilePath)
	var c drainCase
	if runErr == nil {
		found := false
		for _, candidate := range p.ExpectedCases {
			if candidate.CaseID == options.caseID {
				for _, role := range candidate.ExpectedResults {
					if role.SDK == "go" && role.Role == options.role {
						c = candidate
						found = true
					}
				}
			}
		}
		if !found {
			runErr = errors.New("unknown drain case or role")
		}
	}
	var credentials drainCredentials
	if runErr == nil {
		runErr = drainDecode("/input/credentials.json", &credentials)
	}
	if runErr == nil {
		ctx, cancel := context.WithTimeout(context.Background(), p.operationTimeout())
		value.Observations, runErr = executeDrain(ctx, c, p, credentials, options)
		cancel()
	}
	value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if runErr == nil {
		value.Status = "passed"
	} else {
		value.Error = runErr.Error()
	}
	return errors.Join(runErr, writeResult(options.output, value))
}

func executeDrain(ctx context.Context, c drainCase, p drainProfile, credentials drainCredentials, options driverOptions) (map[string]any, error) {
	observations := map[string]any{"permanentEndpoint": options.address}
	topic := "compat/drain/" + c.CaseID + "/message"
	willTopic := "compat/drain/" + c.CaseID + "/will"
	payload := []byte("drain-message/" + c.CaseID)
	var observer *drainSafeClient
	if c.Kind == "cluster_drain" {
		var err error
		observer, err = newDrainSafeClient(ctx, p.Cluster.ObserverEndpoint, p.ClientIDs.Observer, p, credentials, "", nil)
		if err != nil {
			return observations, err
		}
		defer observer.close()
		if _, err := observer.await(ctx); err != nil {
			return observations, err
		}
		if _, err := observer.manager.Subscribe(ctx, &paho5.Subscribe{Subscriptions: []paho5.SubscribeOptions{{Topic: willTopic, QoS: 0}}}); err != nil {
			return observations, err
		}
	} else {
		willTopic = ""
	}
	client, err := newDrainSafeClient(ctx, options.address, p.ClientIDs.Client, p, credentials, willTopic, []byte("drain-will/"+c.CaseID))
	if err != nil {
		return observations, err
	}
	defer client.close()
	if _, err := client.await(ctx); err != nil {
		return observations, err
	}
	if c.Kind == "cluster_drain" {
		if _, err := client.manager.Subscribe(ctx, &paho5.Subscribe{Subscriptions: []paho5.SubscribeOptions{{Topic: topic, QoS: 1}}}); err != nil {
			return observations, err
		}
		if err := writeJSONAtomic(filepath.Join(options.output, "drain-ready.json"), readinessSignal{SchemaVersion: 1, CaseID: c.CaseID, SDK: "go", Role: "client", Protocol: "mqtt5", Ready: true}); err != nil {
			return observations, err
		}
	}
	var decision drainDecision
	select {
	case decision = <-client.disconnected:
	case <-ctx.Done():
		return observations, ctx.Err()
	}
	observations["disconnectSurface"] = decision.surface
	observations["disconnectReasonCode"] = decision.reason
	observations["serverReference"] = decision.reference
	observations["serverReferenceAccepted"] = decision.accepted
	if decision.sdkError != "" {
		observations["sdkError"] = decision.sdkError
	}
	if c.Kind == "malformed_disconnect" {
		observations["vector"] = c.Vector
	}
	ack, err := client.await(ctx)
	if err != nil {
		return observations, err
	}
	observations["reconnectEndpoint"] = ack.endpoint
	if c.Kind == "malformed_disconnect" {
		return observations, waitContext(ctx, 500*time.Millisecond)
	}
	observations["reconnectElapsedMillis"] = ack.at.Sub(decision.at).Milliseconds()
	observations["sessionPresent"] = ack.present
	publisher, err := newDrainSafeClient(ctx, p.Cluster.ObserverEndpoint, p.ClientIDs.Publisher, p, credentials, "", nil)
	if err != nil {
		return observations, err
	}
	defer publisher.close()
	if _, err := publisher.await(ctx); err != nil {
		return observations, err
	}
	if _, err := publisher.manager.Publish(ctx, &paho5.Publish{Topic: topic, QoS: 1, Payload: payload}); err != nil {
		return observations, err
	}
	for {
		select {
		case message := <-client.messages:
			if message.topic == topic && bytes.Equal(message.payload, payload) {
				observations["postRedirectDelivery"] = true
				goto delivered
			}
		case <-ctx.Done():
			return observations, ctx.Err()
		}
	}
delivered:
	deadline := decision.at.Add(time.Duration(p.WillDelaySeconds+1) * time.Second)
	if err := waitContext(ctx, time.Until(deadline)); err != nil {
		return observations, err
	}
	observations["willReceived"] = false
	for {
		select {
		case message := <-observer.messages:
			if message.topic == willTopic {
				observations["willReceived"] = true
				return observations, errors.New("unexpected drain Will")
			}
		default:
			observations["willClassification"] = "suppressed_reconnected_within_delay"
			return observations, nil
		}
	}
}
