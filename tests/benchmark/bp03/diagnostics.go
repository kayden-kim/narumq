package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
)

type diagnosticSample struct {
	Node       string    `json:"node"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
	Path       string    `json:"path,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// startDiagnosticSampler samples independently of workload cancellation so a
// failed warmup still has a final observation. The returned stop owns the join.
func startDiagnosticSampler(management []string, token, outputDir string) (func() error, error) {
	writer, err := jsonl.Open(filepath.Join(outputDir, "diagnostics.jsonl"))
	if err != nil {
		return nil, err
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: time.Second}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		defer client.CloseIdleConnections()
		var result error
		sequence := 0
		sample := func() {
			sequence++
			for index, address := range management {
				record := diagnosticSample{Node: fmt.Sprintf("node%d", index+1), StartedAt: time.Now().UTC()}
				body, err := fetchDiagnosticMetrics(client, address, token)
				record.FinishedAt = time.Now().UTC()
				if err != nil {
					record.Error = err.Error()
				} else {
					record.Path = fmt.Sprintf("metrics/node%d-diagnostic-%06d.prom", index+1, sequence)
					if err := os.WriteFile(filepath.Join(outputDir, filepath.FromSlash(record.Path)), body, 0o644); err != nil {
						record.Error = err.Error()
						result = errors.Join(result, err)
					}
				}
				result = errors.Join(result, writer.Write(record))
			}
		}
		sample()
		for {
			select {
			case <-stop:
				sample()
				done <- errors.Join(result, writer.Close())
				return
			case <-ticker.C:
				sample()
			}
		}
	}()
	return func() error {
		close(stop)
		return <-done
	}, nil
}

func fetchDiagnosticMetrics(client *http.Client, address, token string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, "http://"+address+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics returned %s", response.Status)
	}
	return io.ReadAll(response.Body)
}
