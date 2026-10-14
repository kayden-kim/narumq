package rollout

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	rolloutapp "narumq/internal/application/rollout"
)

const maxReadinessBody = 8192

type PodReadinessProbe struct {
	client *http.Client
}

func NewPodReadinessProbe() *PodReadinessProbe {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &PodReadinessProbe{
		client: &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

func (probe *PodReadinessProbe) Ready(ctx context.Context, podIP string) error {
	ip := net.ParseIP(podIP)
	if ip == nil {
		return errors.New("Pod IP is invalid")
	}
	endpoint := (&url.URL{Scheme: "http", Host: net.JoinHostPort(ip.String(), strconv.Itoa(int(managementServicePort))), Path: "/health/ready"}).String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.New("Pod readiness request could not be constructed")
	}
	response, err := probe.client.Do(request)
	if err != nil {
		return errors.New("Pod readiness endpoint is unavailable")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxReadinessBody))
	if response.StatusCode != http.StatusOK {
		return errors.New("Pod readiness endpoint did not return HTTP 200")
	}
	return nil
}

var _ rolloutapp.ReadinessProbe = (*PodReadinessProbe)(nil)
