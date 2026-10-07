package environment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ReadStats parses broker-stats.jsonl and validates every numeric Docker field.
func ReadStats(path string) ([]StatsSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open stats %q: %w", path, err)
	}
	defer file.Close()

	var samples []StatsSample
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		sample, err := decodeStatsLine([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("parse stats line %d: %w", lineNumber, err)
		}
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read stats %q: %w", path, err)
	}
	return samples, nil
}

func decodeStatsLine(data []byte) (StatsSample, error) {
	var raw struct {
		CollectedAt string         `json:"collectedAt"`
		Container   string         `json:"container"`
		Docker      map[string]any `json:"docker"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return StatsSample{}, fmt.Errorf("decode JSON: %w", err)
	}
	if strings.TrimSpace(raw.CollectedAt) == "" {
		return StatsSample{}, errors.New("collectedAt is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, raw.CollectedAt); err != nil {
		return StatsSample{}, fmt.Errorf("invalid collectedAt: %w", err)
	}
	if strings.TrimSpace(raw.Container) == "" {
		return StatsSample{}, errors.New("container is required")
	}
	if raw.Docker == nil {
		return StatsSample{}, errors.New("docker stats object is required")
	}
	cpuText, err := stringField(raw.Docker, "CPUPerc")
	if err != nil {
		return StatsSample{}, err
	}
	memoryText, err := stringField(raw.Docker, "MemUsage")
	if err != nil {
		return StatsSample{}, err
	}
	networkText, err := stringField(raw.Docker, "NetIO")
	if err != nil {
		return StatsSample{}, err
	}
	blockText, err := stringField(raw.Docker, "BlockIO")
	if err != nil {
		return StatsSample{}, err
	}
	cpu, err := parseCPUPercent(cpuText)
	if err != nil {
		return StatsSample{}, fmt.Errorf("CPUPerc: %w", err)
	}
	memoryUsage, memoryLimit, err := parseMemoryUsage(memoryText)
	if err != nil {
		return StatsSample{}, fmt.Errorf("MemUsage: %w", err)
	}
	networkRx, networkTx, err := parseMemoryUsage(networkText)
	if err != nil {
		return StatsSample{}, fmt.Errorf("NetIO: %w", err)
	}
	blockRead, blockWrite, err := parseMemoryUsage(blockText)
	if err != nil {
		return StatsSample{}, fmt.Errorf("BlockIO: %w", err)
	}
	return StatsSample{
		CollectedAt: raw.CollectedAt, Container: raw.Container, Docker: raw.Docker,
		CPUPercent: cpu, MemoryUsageBytes: memoryUsage, MemoryLimitBytes: memoryLimit,
		NetworkRxBytes: networkRx, NetworkTxBytes: networkTx,
		BlockReadBytes: blockRead, BlockWriteBytes: blockWrite,
	}, nil
}

func decodeLiveDockerStatsLine(line, containerName string) ([]StatsSample, error) {
	line = stripDockerStatsANSI(line)
	records := strings.FieldsFunc(line, func(r rune) bool { return r == '\r' || r == '\n' })
	samples := make([]StatsSample, 0, len(records))
	for _, record := range records {
		record = strings.TrimSpace(record)
		if record == "" {
			continue
		}
		object := fmt.Sprintf("{\"collectedAt\":%q,\"container\":%q,\"docker\":%s}", time.Now().UTC().Format(time.RFC3339Nano), containerName, record)
		sample, err := decodeStatsLine([]byte(object))
		if err != nil {
			return nil, fmt.Errorf("decode live Docker stats record: %w", err)
		}
		samples = append(samples, sample)
	}
	return samples, nil
}

func stripDockerStatsANSI(value string) string {
	var clean strings.Builder
	clean.Grow(len(value))
	for i := 0; i < len(value); {
		if value[i] == 0x1b {
			i++
			if i == len(value) {
				break
			}
			switch value[i] {
			case '[':
				i++
				for i < len(value) && (value[i] < 0x40 || value[i] > 0x7e) {
					i++
				}
				if i < len(value) {
					i++
				}
			case ']':
				i++
				for i < len(value) {
					if value[i] == 0x07 {
						i++
						break
					}
					if value[i] == 0x1b && i+1 < len(value) && value[i+1] == '\\' {
						i += 2
						break
					}
					i++
				}
			default:
				i++
			}
			continue
		}
		if value[i] < 0x20 && value[i] != '\r' && value[i] != '\n' && value[i] != '\t' {
			i++
			continue
		}
		clean.WriteByte(value[i])
		i++
	}
	return clean.String()
}

func stringField(values map[string]any, name string) (string, error) {
	value, ok := values[name]
	if !ok {
		return "", fmt.Errorf("docker stats field %q is required", name)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("docker stats field %q must be a string", name)
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("docker stats field %q must not be empty", name)
	}
	return text, nil
}

func parseCPUPercent(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if !strings.HasSuffix(value, "%") {
		return 0, fmt.Errorf("invalid percentage %q", value)
	}
	number := strings.TrimSpace(strings.TrimSuffix(value, "%"))
	if number == "" || strings.HasPrefix(number, "+") || strings.HasPrefix(number, "-") {
		return 0, fmt.Errorf("invalid percentage %q", value)
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
		return 0, fmt.Errorf("invalid percentage %q", value)
	}
	return parsed, nil
}

func parseMemoryUsage(value string) (int64, int64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected usage and limit separated by one slash, got %q", value)
	}
	usage, err := parseByteQuantity(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("usage: %w", err)
	}
	limit, err := parseByteQuantity(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("limit: %w", err)
	}
	return usage, limit, nil
}

func parseByteQuantity(value string) (int64, error) {
	fields := strings.Fields(value)
	if len(fields) != 1 {
		return 0, fmt.Errorf("invalid byte quantity %q", value)
	}
	text := fields[0]
	units := []struct {
		suffix string
		factor float64
	}{
		{suffix: "TiB", factor: 1 << 40},
		{suffix: "TB", factor: 1e12},
		{suffix: "GiB", factor: 1 << 30},
		{suffix: "GB", factor: 1e9},
		{suffix: "MiB", factor: 1 << 20},
		{suffix: "MB", factor: 1e6},
		{suffix: "KiB", factor: 1 << 10},
		{suffix: "kB", factor: 1e3},
		{suffix: "KB", factor: 1e3},
		{suffix: "B", factor: 1},
	}
	for _, unit := range units {
		if !strings.HasSuffix(text, unit.suffix) {
			continue
		}
		number := strings.TrimSuffix(text, unit.suffix)
		if number == "" || strings.HasPrefix(number, "+") || strings.HasPrefix(number, "-") {
			return 0, fmt.Errorf("invalid byte quantity %q", value)
		}
		parsed, err := strconv.ParseFloat(number, 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
			return 0, fmt.Errorf("invalid byte quantity %q", value)
		}
		bytes := parsed * unit.factor
		if math.IsInf(bytes, 0) || math.IsNaN(bytes) || bytes > float64(math.MaxInt64) {
			return 0, fmt.Errorf("byte quantity overflows int64: %q", value)
		}
		return int64(math.Round(bytes)), nil
	}
	return 0, fmt.Errorf("unknown byte unit in %q", value)
}

func startStatsSampler(ctx context.Context, containerName, outputPath string, skipWhenStopped bool) (func(), <-chan error) {
	if ctx == nil {
		ctx = context.Background()
	}
	samplerContext, cancel := context.WithCancel(ctx)
	stopRequested := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopRequested)
			cancel()
		})
	}
	isStopped := func() bool {
		select {
		case <-stopRequested:
			return true
		default:
			return false
		}
	}
	done := make(chan error, 1)
	go func() {
		file, err := os.Create(outputPath)
		if err != nil {
			if isStopped() {
				done <- nil
			} else {
				done <- err
			}
			return
		}
		defer file.Close()
		writer := bufio.NewWriterSize(file, 64*1024)
		writeSample := func() error {
			output, err := commandOutput(samplerContext, "", "docker", "stats", "--no-stream", "--format", "{{json .}}", containerName)
			if err != nil {
				return err
			}
			line := strings.TrimSpace(string(output))
			if line == "" {
				return errors.New("docker stats returned no sample")
			}
			object := fmt.Sprintf("{\"collectedAt\":%q,\"container\":%q,\"docker\":%s}", time.Now().UTC().Format(time.RFC3339Nano), containerName, line)
			sample, err := decodeStatsLine([]byte(object))
			if err != nil {
				return err
			}
			if err := json.NewEncoder(writer).Encode(sample); err != nil {
				return err
			}
			return writer.Flush()
		}
		skipSampleError := func(sampleErr error) (bool, error) {
			if isStopped() {
				return true, nil
			}
			if !skipWhenStopped {
				return false, sampleErr
			}
			stopped, inspectErr := containerStopped(samplerContext, containerName)
			if inspectErr != nil {
				return false, errors.Join(sampleErr, inspectErr)
			}
			if stopped {
				return true, nil
			}
			// The broker can finish a planned restart between the failed stats call and inspect.
			if retryErr := writeSample(); retryErr != nil {
				return false, errors.Join(sampleErr, retryErr)
			}
			return true, nil
		}
		if sampleErr := writeSample(); sampleErr != nil {
			skip, err := skipSampleError(sampleErr)
			if err != nil {
				done <- err
				return
			}
			if !skip {
				done <- sampleErr
				return
			}
			if isStopped() {
				done <- nil
				return
			}
		}
		ticker := time.NewTicker(DefaultStatsSamplePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-samplerContext.Done():
				if isStopped() {
					done <- nil
				} else {
					done <- samplerContext.Err()
				}
				return
			case <-ticker.C:
				if sampleErr := writeSample(); sampleErr != nil {
					skip, err := skipSampleError(sampleErr)
					if err != nil {
						done <- err
						return
					}
					if !skip {
						done <- sampleErr
						return
					}
					if isStopped() {
						done <- nil
						return
					}
				}
			}
		}
	}()
	return stop, done
}

func splitLiveDockerStatsRecords(data []byte, atEOF bool) (int, []byte, error) {
	for index, value := range data {
		if value != '\r' && value != '\n' {
			continue
		}
		if index == 0 {
			return 1, nil, nil
		}
		return index + 1, data[:index], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func startLiveStatsSampler(ctx context.Context, containerName, outputPath string) (func(), <-chan error) {
	if ctx == nil {
		ctx = context.Background()
	}
	samplerContext, cancel := context.WithCancel(ctx)
	stopRequested := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopRequested)
			cancel()
		})
	}
	isStopped := func() bool {
		select {
		case <-stopRequested:
			return true
		default:
			return false
		}
	}
	done := make(chan error, 1)
	go func() {
		file, err := os.Create(outputPath)
		if err != nil {
			if isStopped() {
				done <- nil
			} else {
				done <- err
			}
			return
		}
		defer file.Close()
		writer := bufio.NewWriterSize(file, 64*1024)
		command := exec.CommandContext(samplerContext, "docker", "stats", "--format", "{{json .}}", containerName)
		stdout, err := command.StdoutPipe()
		if err != nil {
			done <- fmt.Errorf("open docker stats output for %s: %w", containerName, err)
			return
		}
		var stderr bytes.Buffer
		command.Stderr = &stderr
		if err := command.Start(); err != nil {
			if isStopped() {
				done <- nil
			} else {
				done <- fmt.Errorf("start docker stats for %s: %w", containerName, err)
			}
			return
		}

		scanner := bufio.NewScanner(stdout)
		scanner.Split(splitLiveDockerStatsRecords)
		scanner.Buffer(make([]byte, 1024), 1<<20)
		samples := 0
		var sampleErr error
		for scanner.Scan() {
			decoded, err := decodeLiveDockerStatsLine(scanner.Text(), containerName)
			if err != nil {
				sampleErr = err
				break
			}
			for _, sample := range decoded {
				if err := json.NewEncoder(writer).Encode(sample); err != nil {
					sampleErr = err
					break
				}
				if err := writer.Flush(); err != nil {
					sampleErr = err
					break
				}
				samples++
			}
			if sampleErr != nil {
				break
			}
		}
		scannerErr := scanner.Err()
		if sampleErr != nil {
			cancel()
		}
		waitErr := command.Wait()
		if isStopped() {
			done <- nil
			return
		}
		if sampleErr != nil {
			done <- fmt.Errorf("write Docker stats sample for %s: %w", containerName, sampleErr)
			return
		}
		if scannerErr != nil {
			done <- fmt.Errorf("read Docker stats stream for %s: %w", containerName, scannerErr)
			return
		}
		if waitErr != nil {
			message := strings.TrimSpace(stderr.String())
			if message != "" {
				done <- fmt.Errorf("Docker stats stream for %s failed: %w: %s", containerName, waitErr, message)
			} else {
				done <- fmt.Errorf("Docker stats stream for %s failed: %w", containerName, waitErr)
			}
			return
		}
		if samples == 0 {
			done <- fmt.Errorf("Docker stats returned no samples for %s", containerName)
			return
		}
		done <- fmt.Errorf("Docker stats stream for %s ended unexpectedly", containerName)
	}()
	return stop, done
}

func containerStopped(ctx context.Context, containerName string) (bool, error) {
	output, err := commandOutput(ctx, "", "docker", "inspect", "--format", "{{.State.Running}}", containerName)
	if err != nil {
		return false, fmt.Errorf("inspect container %s state: %w", containerName, err)
	}
	running, err := strconv.ParseBool(strings.TrimSpace(string(output)))
	if err != nil {
		return false, fmt.Errorf("parse container %s running state: %w", containerName, err)
	}
	return !running, nil
}

func appendBrokerStatsSample(ctx context.Context, containerName, outputPath string) error {
	output, err := commandOutput(ctx, "", "docker", "stats", "--no-stream", "--format", "{{json .}}", containerName)
	if err != nil {
		return err
	}
	line := strings.TrimSpace(string(output))
	if line == "" {
		return errors.New("docker stats returned no sample")
	}
	object := fmt.Sprintf("{\"collectedAt\":%q,\"container\":%q,\"docker\":%s}", time.Now().UTC().Format(time.RFC3339Nano), containerName, line)
	sample, err := decodeStatsLine([]byte(object))
	if err != nil {
		return err
	}
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriterSize(file, 64*1024)
	if err := json.NewEncoder(writer).Encode(sample); err != nil {
		return err
	}
	return writer.Flush()
}
