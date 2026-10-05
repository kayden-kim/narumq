package main

import (
	"crypto/tls"
	"fmt"
	"net"

	"narumq/internal/adapters/clock"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	portsmetrics "narumq/internal/application/ports/metrics"
)

// openMQTTListener binds address, applies admission limits to the raw TCP socket when
// either limit is set, and wraps TLS outside admission so the MQTT server sees *tls.Conn.
func openMQTTListener(address string, tlsConfig *tls.Config, maxConnections, connectionRate int, recorder portsmetrics.ListenerAdmissionRecorder) (net.Listener, error) {
	bound, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	listener := bound
	if maxConnections != 0 || connectionRate != 0 {
		transport := portsmetrics.TransportTCP
		if tlsConfig != nil {
			transport = portsmetrics.TransportTLS
		}
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

func validateListenerAdmissionFlags(tcpAddress, tlsAddress string, tcpMaxConnections, tcpConnectionRate, tlsMaxConnections, tlsConnectionRate int) error {
	values := []struct {
		name  string
		value int
	}{
		{"tcp-max-connections", tcpMaxConnections}, {"tcp-connection-rate", tcpConnectionRate},
		{"tls-max-connections", tlsMaxConnections}, {"tls-connection-rate", tlsConnectionRate},
	}
	for _, setting := range values {
		if setting.value < 0 || setting.value > tcpadapter.MaxListenerAdmissionLimit {
			return fmt.Errorf("-%s must be between 0 and %d", setting.name, tcpadapter.MaxListenerAdmissionLimit)
		}
	}
	if tcpAddress == "" && (tcpMaxConnections != 0 || tcpConnectionRate != 0) {
		return fmt.Errorf("-tcp-max-connections and -tcp-connection-rate require -tcp-listen")
	}
	if tlsAddress == "" && (tlsMaxConnections != 0 || tlsConnectionRate != 0) {
		return fmt.Errorf("-tls-max-connections and -tls-connection-rate require -tls-listen")
	}
	return nil
}
