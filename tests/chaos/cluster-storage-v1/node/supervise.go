package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func runSupervise(args []string) error {
	flags := newFlagSet("supervise")
	broker := flags.String("broker", "", "broker executable")
	brokerConfig := flags.String("broker-config", "", "broker cluster configuration")
	dataDir := flags.String("data-dir", "", "broker data directory")
	jwksFile := flags.String("jwks-file", "", "JWKS document served to the broker")
	jwksListen := flags.String("jwks-listen", "", "loopback JWKS listener address")
	slowFSBacking := flags.String("slowfs-backing", "", "backing directory for the delayed filesystem")
	slowFSMount := flags.String("slowfs-mount", "", "delayed filesystem mountpoint")
	delayFile := flags.String("delay-file", "", "atomic I/O delay control file")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"broker": *broker, "broker-config": *brokerConfig, "data-dir": *dataDir,
		"jwks-file": *jwksFile, "jwks-listen": *jwksListen, "delay-file": *delayFile,
	} {
		if err := requireFlag(name, value); err != nil {
			return err
		}
	}
	if (*slowFSBacking == "") != (*slowFSMount == "") {
		return errors.New("--slowfs-backing and --slowfs-mount must be provided together")
	}

	delay := &ioDelay{}
	delay.refresh(*delayFile)
	delayContext, stopDelayRefresh := context.WithCancel(context.Background())
	go delay.refreshLoop(delayContext, *delayFile)

	var fuseServer *fuse.Server
	var fuseDone chan struct{}
	var brokerCommand *exec.Cmd
	var brokerExit <-chan error
	var httpServer *http.Server
	cleanup := func() {
		stopDelayRefresh()
		if brokerCommand != nil {
			stopBroker(brokerCommand, brokerExit)
			brokerCommand = nil
		}
		if fuseServer != nil {
			_ = fuseServer.Unmount()
			<-fuseDone
		}
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(ctx)
		}
	}
	defer cleanup()

	if *slowFSBacking != "" {
		var err error
		fuseServer, err = mountSlowFS(*slowFSBacking, *slowFSMount, delay)
		if err != nil {
			return fmt.Errorf("mount delayed filesystem: %w", err)
		}
		fuseDone = make(chan struct{})
		go func() {
			fuseServer.Wait()
			close(fuseDone)
		}()
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create broker data directory: %w", err)
	}

	listener, err := net.Listen("tcp", *jwksListen)
	if err != nil {
		return fmt.Errorf("listen for JWKS requests: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/jwks.json" {
			http.NotFound(writer, request)
			return
		}
		http.ServeFile(writer, request, *jwksFile)
	})
	httpServer = &http.Server{Handler: mux}
	httpErrors := make(chan error, 1)
	go func() {
		err := httpServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrors <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)
	defer signal.Stop(signals)
	brokerCommand, brokerExit, err = startBroker(*broker, *brokerConfig)
	if err != nil {
		return err
	}

	for {
		select {
		case received := <-signals:
			if received == syscall.SIGUSR1 {
				if brokerCommand == nil {
					continue
				}
				previousExitCode := stopBroker(brokerCommand, brokerExit)
				brokerCommand = nil
				brokerExit = nil
				if err := writeJSON(map[string]any{"event": "broker-restart", "previousExitCode": previousExitCode}); err != nil {
					return err
				}
				brokerCommand, brokerExit, err = startBroker(*broker, *brokerConfig)
				if err != nil {
					return err
				}
				continue
			}
			if brokerCommand != nil {
				stopBroker(brokerCommand, brokerExit)
				brokerCommand = nil
			}
			return nil
		case err := <-brokerExit:
			if brokerCommand == nil {
				continue
			}
			if err := writeJSON(map[string]any{"event": "broker-exit", "exitCode": processExitCode(err)}); err != nil {
				return err
			}
			brokerCommand = nil
			brokerExit = nil
		case err := <-httpErrors:
			return fmt.Errorf("serve JWKS endpoint: %w", err)
		case <-fuseDone:
			return errors.New("delayed filesystem stopped unexpectedly")
		}
	}
}

func startBroker(binary, config string) (*exec.Cmd, <-chan error, error) {
	command := exec.Command(binary, "-cluster-config", config)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return nil, nil, fmt.Errorf("start broker: %w", err)
	}
	exit := make(chan error, 1)
	go func() {
		exit <- command.Wait()
		close(exit)
	}()
	return command, exit, nil
}

func stopBroker(command *exec.Cmd, exit <-chan error) int {
	if command == nil {
		return 0
	}
	_ = command.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-exit:
		return processExitCode(err)
	case <-timer.C:
		_ = command.Process.Kill()
		return processExitCode(<-exit)
	}
}

func processExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return 1
	}
	if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
		if status.Signaled() {
			return 128 + int(status.Signal())
		}
		return status.ExitStatus()
	}
	return exitError.ExitCode()
}
