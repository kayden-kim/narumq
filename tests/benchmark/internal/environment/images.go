package environment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func buildImages(ctx context.Context, options BuildOptions) (ImageSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(options.BenchmarkID) == "" {
		return ImageSet{}, errors.New("benchmark ID is required")
	}
	if strings.TrimSpace(options.RunID) == "" {
		return ImageSet{}, errors.New("run ID is required")
	}
	if strings.TrimSpace(options.RepoRoot) == "" {
		return ImageSet{}, errors.New("repository root is required")
	}
	if strings.TrimSpace(options.ArtifactDir) == "" {
		return ImageSet{}, errors.New("artifact directory is required")
	}
	runnerTarget := options.RunnerTarget
	if runnerTarget == "" {
		runnerTarget = options.BenchmarkID + "-runner"
	}
	if runnerTarget != "bp01-runner" && runnerTarget != "bp05-runner" && runnerTarget != "bp03-runner" && runnerTarget != "bp04-runner" && runnerTarget != "bp06-runner" && runnerTarget != "bp07-runner" {
		return ImageSet{}, fmt.Errorf("unsupported runner target %q", runnerTarget)
	}
	if err := os.MkdirAll(options.ArtifactDir, 0o755); err != nil {
		return ImageSet{}, fmt.Errorf("create build artifact directory: %w", err)
	}

	images := ImageSet{
		NaruMQReference: "narumq-" + options.BenchmarkID + "-narumq:" + options.RunID,
		RunnerReference: "narumq-" + options.BenchmarkID + "-runner:" + options.RunID,
		RunnerTarget:    runnerTarget,
	}
	builds := []struct {
		name      string
		target    string
		reference string
	}{
		{name: "narumq", target: "narumq", reference: images.NaruMQReference},
		{name: "runner", target: runnerTarget, reference: images.RunnerReference},
	}
	for _, build := range builds {
		output, err := commandOutput(ctx, options.RepoRoot, "docker", "build", "--progress=plain", "--target", build.target, "--tag", build.reference, "--file", filepath.Join("tests", "benchmark", "Dockerfile"), ".")
		if writeErr := os.WriteFile(filepath.Join(options.ArtifactDir, "docker-build-"+build.name+".log"), output, 0o644); writeErr != nil {
			return ImageSet{}, fmt.Errorf("write Docker build log for %s: %w", build.name, writeErr)
		}
		if err != nil {
			return ImageSet{}, fmt.Errorf("build %s image: %w", build.target, err)
		}
	}
	var err error
	if images.NaruMQID, err = dockerImageID(ctx, options.RepoRoot, images.NaruMQReference); err != nil {
		return ImageSet{}, err
	}
	if images.RunnerID, err = dockerImageID(ctx, options.RepoRoot, images.RunnerReference); err != nil {
		return ImageSet{}, err
	}
	return images, nil
}

// EnsureEMQXImage pulls the selected immutable comparator and verifies its digest.
func EnsureEMQXImage(ctx context.Context, artifactDir, imageReference string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(artifactDir) == "" {
		return errors.New("artifact directory is required")
	}
	parts := strings.SplitN(imageReference, "@", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[1], "sha256:") {
		return errors.New("EMQX image must be pinned by digest")
	}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	output, err := commandOutput(ctx, "", "docker", "pull", imageReference)
	if writeErr := os.WriteFile(filepath.Join(artifactDir, "docker-pull-emqx.log"), output, 0o644); writeErr != nil {
		return fmt.Errorf("write EMQX pull log: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("pull exact EMQX image: %w", err)
	}
	digests, err := dockerRepoDigests(ctx, "", imageReference)
	if err != nil {
		return err
	}
	repository := parts[0]
	if tag := strings.LastIndex(repository, ":"); tag > strings.LastIndex(repository, "/") {
		repository = repository[:tag]
	}
	want := repository + "@" + parts[1]
	for _, digest := range digests {
		if digest == want {
			return nil
		}
	}
	return fmt.Errorf("EMQX image repo digest mismatch: got %v, want %s", digests, want)
}

func dockerImageID(ctx context.Context, workingDirectory, imageReference string) (string, error) {
	output, err := commandOutput(ctx, workingDirectory, "docker", "image", "inspect", "--format", "{{.Id}}", imageReference)
	if err != nil {
		return "", fmt.Errorf("inspect image %s: %w", imageReference, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func dockerRepoDigests(ctx context.Context, workingDirectory, imageReference string) ([]string, error) {
	output, err := commandOutput(ctx, workingDirectory, "docker", "image", "inspect", "--format", "{{json .RepoDigests}}", imageReference)
	if err != nil {
		return nil, fmt.Errorf("inspect image digests %s: %w", imageReference, err)
	}
	var digests []string
	if err := json.Unmarshal(output, &digests); err != nil {
		return nil, fmt.Errorf("decode image digests %s: %w", imageReference, err)
	}
	return digests, nil
}
