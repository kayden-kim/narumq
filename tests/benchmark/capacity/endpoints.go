package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/mqttclient"
)

const resolveTTL = time.Second

// endpoints dials the single load-balanced MQTT endpoint. The host is re-resolved at most once
// per second and every connection picks a random resolved address, so a DNS alias shared by
// all brokers (or an NLB name with one address per zone) spreads connections and a refused
// address (a killed broker) falls through to the next one.
type endpoints struct {
	plain     string
	tlsAddr   string
	tlsConfig *tls.Config

	mu    sync.Mutex
	cache map[string]resolvedHost
}

type resolvedHost struct {
	addresses []string
	at        time.Time
}

func newEndpoints(plain, tlsAddr, caFile, serverName string) (*endpoints, error) {
	for _, address := range []string{plain, tlsAddr} {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return nil, fmt.Errorf("invalid endpoint %q", address)
		}
		if value, err := strconv.ParseUint(port, 10, 16); err != nil || value == 0 {
			return nil, fmt.Errorf("invalid endpoint %q: bad port", address)
		}
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("TLS CA file contains no certificate")
	}
	if serverName == "" {
		return nil, errors.New("TLS server name is required")
	}
	return &endpoints{
		plain: plain, tlsAddr: tlsAddr, cache: make(map[string]resolvedHost),
		tlsConfig: &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12},
	}, nil
}

func (e *endpoints) resolve(ctx context.Context, host string, force bool) ([]string, error) {
	if net.ParseIP(host) != nil {
		return []string{host}, nil
	}
	e.mu.Lock()
	cached, ok := e.cache[host]
	e.mu.Unlock()
	if ok && !force && time.Since(cached.at) < resolveTTL {
		return cached.addresses, nil
	}
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.cache[host] = resolvedHost{addresses: addresses, at: time.Now()}
	e.mu.Unlock()
	return addresses, nil
}

// isConnAckRefusal reports a CONNACK rejection, which must not be retried on another address.
func isConnAckRefusal(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unexpected CONNACK")
}

// connect dials one resolved address, falling through to the others on dial or TLS failure.
func (e *endpoints) connect(ctx context.Context, rng *rand.Rand, useTLS bool, options mqttclient.ConnectOptions) (*mqttclient.Client, mqttclient.ConnectTiming, error) {
	address := e.plain
	if useTLS {
		address = e.tlsAddr
		options.TLSConfig = e.tlsConfig
	}
	host, port, _ := net.SplitHostPort(address)
	var lastErr error
	for attempt := range 2 {
		addresses, err := e.resolve(ctx, host, attempt > 0)
		if err != nil {
			lastErr = err
			continue
		}
		start := rng.IntN(len(addresses))
		for offset := range addresses {
			options.Address = net.JoinHostPort(addresses[(start+offset)%len(addresses)], port)
			started := time.Now()
			client, timing, err := mqttclient.Connect(ctx, options)
			if err == nil {
				timing.StartedAt, timing.Latency = started, timing.ConnAckAt.Sub(started)
				return client, timing, nil
			}
			lastErr = err
			if isConnAckRefusal(err) || ctx.Err() != nil {
				return nil, mqttclient.ConnectTiming{}, err
			}
		}
	}
	return nil, mqttclient.ConnectTiming{}, lastErr
}

// connackReason extracts a non-zero CONNACK reason code from a refusal error.
func connackReason(err error) (byte, bool) {
	if err == nil {
		return 0, false
	}
	text := err.Error()
	index := strings.Index(text, "ReasonCode:0x")
	if index < 0 {
		return 0, false
	}
	hexadecimal := text[index+len("ReasonCode:0x"):]
	end := 0
	for end < len(hexadecimal) && strings.IndexByte("0123456789abcdefABCDEF", hexadecimal[end]) >= 0 {
		end++
	}
	if end == 0 {
		return 0, false
	}
	reason, parseErr := strconv.ParseUint(hexadecimal[:end], 16, 8)
	return byte(reason), parseErr == nil && reason != 0
}
