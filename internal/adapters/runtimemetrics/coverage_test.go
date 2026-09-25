package runtimemetrics

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
)

var cmmErrWriteFailed = errors.New("cmm write failed")

// cmmFailingWriter accepts okWrites writes and fails every later write. After
// each accepted write it waits until the wall clock advances so the sampler's
// next timestamp is strictly newer and cannot be skipped as a duplicate.
type cmmFailingWriter struct {
	okWrites int
	writes   int
}

func (w *cmmFailingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > w.okWrites {
		return 0, cmmErrWriteFailed
	}
	for start := time.Now(); !time.Now().After(start); {
	}
	return len(p), nil
}

func cmmNewSampler(t *testing.T, interval time.Duration) (*Sampler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	sampler, err := New(path, interval)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sampler.close() })
	return sampler, path
}

func cmmAssertErrorContains(t *testing.T, err error, want []string, reject []string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want failure")
	}
	for _, fragment := range want {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q does not contain %q", err, fragment)
		}
	}
	for _, fragment := range reject {
		if strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q unexpectedly contains %q", err, fragment)
		}
	}
}

func TestCmmNewRejectsUnavailableOrMistypedMetric(t *testing.T) {
	tests := []struct {
		name   string
		spec   metricSpec
		expect string
	}{
		{name: "unavailable", spec: metricSpec{name: "/cmm/missing:bytes", kind: metrics.KindUint64}, expect: `required runtime metric "/cmm/missing:bytes" is unavailable`},
		{name: "wrong kind", spec: metricSpec{name: "/gc/heap/allocs:bytes", kind: metrics.KindFloat64}, expect: `runtime metric "/gc/heap/allocs:bytes" has kind`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := metricSpecs[0]
			metricSpecs[0] = test.spec
			t.Cleanup(func() { metricSpecs[0] = original })

			path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
			sampler, err := New(path, time.Second)
			if sampler != nil {
				t.Fatalf("New() returned sampler for invalid catalog")
			}
			cmmAssertErrorContains(t, err, []string{test.expect}, nil)
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("output stat error = %v, want no file created on catalog failure", statErr)
			}
		})
	}
}

func TestCmmRunWithNilContextOnClosedSamplerReturnsInitialSampleError(t *testing.T) {
	sampler, _ := cmmNewSampler(t, time.Hour)
	if err := sampler.close(); err != nil {
		t.Fatal(err)
	}
	var ctx context.Context
	err := sampler.Run(ctx)
	cmmAssertErrorContains(t, err, []string{"write initial runtime metrics sample", "runtime metrics sampler is closed"}, []string{"flush", "close runtime metrics output"})
}

func TestCmmRunJoinsInitialFlushAndCloseErrors(t *testing.T) {
	sampler, _ := cmmNewSampler(t, time.Hour)
	if err := sampler.file.Close(); err != nil {
		t.Fatal(err)
	}
	err := sampler.Run(context.Background())
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Run() error = %v, want os.ErrClosed", err)
	}
	cmmAssertErrorContains(t, err, []string{"write initial runtime metrics sample", "flush runtime metrics output", "close runtime metrics output"}, nil)
}

func TestCmmRunReportsFinalSampleFailureOnCancellation(t *testing.T) {
	sampler, path := cmmNewSampler(t, time.Hour)
	writer := &cmmFailingWriter{okWrites: 1}
	sampler.writer = bufio.NewWriterSize(writer, 16)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sampler.Run(ctx)
	if !errors.Is(err, cmmErrWriteFailed) {
		t.Fatalf("Run() error = %v, want %v", err, cmmErrWriteFailed)
	}
	cmmAssertErrorContains(t, err, []string{"write final runtime metrics sample", "flush runtime metrics output"}, []string{"initial", "close runtime metrics output"})
	if writer.writes != 2 {
		t.Fatalf("writes = %d, want initial plus failed final", writer.writes)
	}
	if sampler.file != nil {
		t.Fatal("sampler file remains open after Run() failure")
	}
	if data, readErr := os.ReadFile(path); readErr != nil || len(data) != 0 {
		t.Fatalf("output = %q, %v; want empty file because writes went to replaced writer", data, readErr)
	}
}

func TestCmmRunReportsPeriodicSampleFailure(t *testing.T) {
	sampler, _ := cmmNewSampler(t, time.Nanosecond)
	writer := &cmmFailingWriter{okWrites: 2}
	sampler.writer = bufio.NewWriterSize(writer, 16)

	err := sampler.Run(context.Background())
	if !errors.Is(err, cmmErrWriteFailed) {
		t.Fatalf("Run() error = %v, want %v", err, cmmErrWriteFailed)
	}
	cmmAssertErrorContains(t, err, []string{"write runtime metrics sample", "flush runtime metrics output"}, []string{"initial", "final"})
	if writer.writes != 3 {
		t.Fatalf("writes = %d, want initial, one periodic, then failed periodic", writer.writes)
	}
}

func TestCmmCloseReportsCloseErrorWithoutFlushError(t *testing.T) {
	sampler, _ := cmmNewSampler(t, time.Hour)
	if err := sampler.file.Close(); err != nil {
		t.Fatal(err)
	}
	err := sampler.close()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close() error = %v, want os.ErrClosed", err)
	}
	cmmAssertErrorContains(t, err, []string{"close runtime metrics output"}, []string{"flush"})
	if err := sampler.close(); err != nil {
		t.Fatalf("second close() error = %v, want nil", err)
	}
}
