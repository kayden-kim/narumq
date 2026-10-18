package main

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"

	"narumq/internal/adapters/clock"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	websocketadapter "narumq/internal/adapters/websocket"
	portsmetrics "narumq/internal/application/ports/metrics"
)

// openMQTTListener binds the raw socket, applies admission limits, then wraps TLS when configured.
func openMQTTListener(transport, address string, tlsConfig *tls.Config, maxConnections, connectionRate int, recorder portsmetrics.ListenerAdmissionRecorder) (net.Listener, error) {
	bound, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	listener := net.Listener(bound)
	if maxConnections != 0 || connectionRate != 0 {
		listener, err = tcpadapter.NewAdmissionListener(listener, tcpadapter.ListenerAdmissionConfig{
			Transport: transport, MaxConnections: maxConnections, ConnectionRate: connectionRate,
			Clock: clock.SystemClock{}, Recorder: recorder,
		})
		if err != nil {
			_ = bound.Close()
			return nil, err
		}
	}
	if tlsConfig != nil {
		listener, err = tlsadapter.NewListener(listener, tlsConfig)
		if err != nil {
			_ = bound.Close()
			return nil, err
		}
	}
	return listener, nil
}

// openWebSocketListener adds HTTP upgrade handling after socket admission and optional TLS.
func openWebSocketListener(address string, tlsConfig *tls.Config, maxConnections, connectionRate int, recorder portsmetrics.ListenerAdmissionRecorder, logger *slog.Logger) (net.Listener, error) {
	transport := portsmetrics.TransportWS
	if tlsConfig != nil {
		transport = portsmetrics.TransportWSS
	}
	inner, err := openMQTTListener(transport, address, tlsConfig, maxConnections, connectionRate, recorder)
	if err != nil {
		return nil, err
	}
	listener, err := websocketadapter.NewListener(inner, websocketadapter.Config{
		HandshakeTimeout: websocketadapter.HandshakeTimeout,
		Logger:           logger,
	})
	if err != nil {
		_ = inner.Close()
		return nil, err
	}
	return listener, nil
}

type listenerAdmissionFlag struct {
	Name           string
	Address        string
	MaxConnections int
	ConnectionRate int
}

func validateListenerAdmissionFlags(listeners ...listenerAdmissionFlag) error {
	for _, listener := range listeners {
		if listener.MaxConnections < 0 || listener.MaxConnections > tcpadapter.MaxListenerAdmissionLimit {
			return fmt.Errorf("-%s-max-connections must be between 0 and %d", listener.Name, tcpadapter.MaxListenerAdmissionLimit)
		}
		if listener.ConnectionRate < 0 || listener.ConnectionRate > tcpadapter.MaxListenerAdmissionLimit {
			return fmt.Errorf("-%s-connection-rate must be between 0 and %d", listener.Name, tcpadapter.MaxListenerAdmissionLimit)
		}
		if listener.Address == "" && (listener.MaxConnections != 0 || listener.ConnectionRate != 0) {
			return fmt.Errorf("-%s-max-connections and -%s-connection-rate require -%s-listen", listener.Name, listener.Name, listener.Name)
		}
	}
	return nil
}
