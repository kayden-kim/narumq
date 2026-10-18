package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"narumq/internal/adapters/auditlog"
	rolloutadapter "narumq/internal/adapters/rollout"
)

var buildVersion = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("rollout controller stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	namespace, err := requiredEnvironment("POD_NAMESPACE")
	if err != nil {
		return err
	}
	instanceID, err := requiredEnvironment("POD_UID")
	if err != nil {
		return err
	}
	auditDirectory, err := requiredEnvironment("NARUMQ_AUDIT_DIRECTORY")
	if err != nil {
		return err
	}
	registryValue, err := requiredEnvironment("NARUMQ_ALLOWED_OCI_REGISTRIES")
	if err != nil {
		return err
	}
	registries := strings.Split(registryValue, ",")
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{Directory: auditDirectory, MaxFileBytes: auditlog.DefaultMaxFileBytes})
	if err != nil {
		return fmt.Errorf("open durable rollout audit publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			slog.Error("close rollout audit publisher", "error", err.Error())
		}
	}()
	if err := publisher.Ready(); err != nil {
		return fmt.Errorf("durable rollout audit publisher is unavailable: %w", err)
	}
	if buildVersion == "" || buildVersion == "dev" {
		return errors.New("controller build version must be embedded at link time")
	}
	return rolloutadapter.RunController(rolloutadapter.ControllerConfig{
		Namespace: namespace, ProducerID: instanceID, ProducerVersion: buildVersion, Registries: registries,
	}, publisher)
}

func requiredEnvironment(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("required environment variable %s is empty", name)
	}
	return value, nil
}
