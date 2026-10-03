package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

type powerRequest struct {
	kind  string
	torn  bool
	reply chan powerReply
}

type powerReply struct {
	status int
	body   any
}

func runSupervise(args []string) error {
	flags := newFlagSet("supervise")
	broker := flags.String("broker", "", "broker executable")
	brokerConfig := flags.String("broker-config", "", "broker cluster configuration")
	dataDir := flags.String("data-dir", "", "broker data directory")
	jwksFile := flags.String("jwks-file", "", "JWKS document served to the broker")
	jwksListen := flags.String("jwks-listen", "", "loopback JWKS listener address")
	chaosFSBacking := flags.String("chaosfs-backing", "", "backing directory for the chaos filesystem")
	chaosFSMount := flags.String("chaosfs-mount", "", "chaos filesystem mountpoint")
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
	if (*chaosFSBacking == "") != (*chaosFSMount == "") {
		return errors.New("--chaosfs-backing and --chaosfs-mount must be provided together")
	}

	delay := &ioDelay{}
	delay.refresh(*delayFile)
	delayContext, stopDelayRefresh := context.WithCancel(context.Background())
	go delay.refreshLoop(delayContext, *delayFile)
	controller := newChaosController(delay)

	var mount *chaosMount
	var brokerCommand *exec.Cmd
	var brokerExit <-chan error
	var httpServer *http.Server
	cleanup := func() {
		stopDelayRefresh()
		if brokerCommand != nil {
			stopBroker(brokerCommand, brokerExit)
			brokerCommand = nil
		}
		if mount != nil {
			_ = mount.unmount()
		}
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(ctx)
		}
	}
	defer cleanup()

	if *chaosFSBacking != "" {
		var err error
		mount, err = mountChaosFS(*chaosFSBacking, *chaosFSMount, controller)
		if err != nil {
			return fmt.Errorf("mount chaos filesystem: %w", err)
		}
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create broker data directory: %w", err)
	}
	if err := syncDirectoryPath(filepath.Dir(*dataDir)); err != nil {
		return fmt.Errorf("sync broker data parent directory: %w", err)
	}
	listener, err := net.Listen("tcp", *jwksListen)
	if err != nil {
		return fmt.Errorf("listen for JWKS requests: %w", err)
	}
	powerRequests := make(chan powerRequest)
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/jwks.json" {
			http.NotFound(writer, request)
			return
		}
		http.ServeFile(writer, request, *jwksFile)
	})
	mux.HandleFunc("/chaos/storage", func(writer http.ResponseWriter, request *http.Request) {
		handleStorageStatus(writer, request, mount, controller)
	})
	mux.HandleFunc("/chaos/storage/mode", func(writer http.ResponseWriter, request *http.Request) {
		handleStorageMode(writer, request, mount, controller)
	})
	mux.HandleFunc("/chaos/power-cut", func(writer http.ResponseWriter, request *http.Request) {
		handlePowerCut(writer, request, mount, powerRequests)
	})
	mux.HandleFunc("/chaos/power-on", func(writer http.ResponseWriter, request *http.Request) {
		handlePowerOn(writer, request, mount, powerRequests)
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
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2)
	defer signal.Stop(signals)
	mountDone := func() <-chan struct{} {
		if mount == nil {
			return nil
		}
		return mount.doneChannel()
	}
	brokerCommand, brokerExit, err = startBroker(*broker, *brokerConfig)
	if err != nil {
		return err
	}

	for {
		select {
		case received := <-signals:
			if received == syscall.SIGUSR2 {
				if brokerCommand == nil {
					return errors.New("cannot crash a broker that is not running")
				}
				if err := brokerCommand.Process.Kill(); err != nil {
					return fmt.Errorf("kill broker process: %w", err)
				}
				code := processExitCode(<-brokerExit)
				brokerCommand = nil
				brokerExit = nil
				if err := writeJSON(map[string]any{"event": "broker-crash", "exitCode": code}); err != nil {
					return err
				}
				continue
			}
			if received == syscall.SIGUSR1 {
				if controller.status().PoweredOff {
					if err := writeJSON(map[string]any{"event": "broker-restart-ignored", "reason": "powered-off"}); err != nil {
						return err
					}
					continue
				}
				var previousExitCode *int
				if brokerCommand != nil {
					code := stopBroker(brokerCommand, brokerExit)
					previousExitCode = &code
					brokerCommand = nil
					brokerExit = nil
				}
				event := map[string]any{"event": "broker-restart"}
				if previousExitCode != nil {
					event["previousExitCode"] = *previousExitCode
				}
				if err := writeJSON(event); err != nil {
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
				brokerExit = nil
			}
			return nil
		case request := <-powerRequests:
			if err := processPowerRequest(request, controller, mount, *broker, *brokerConfig, &brokerCommand, &brokerExit); err != nil {
				return err
			}
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
		case <-mountDone():
			return errors.New("chaos filesystem stopped unexpectedly")
		}
	}
}
func syncDirectoryPath(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close directory: %w", closeErr)
	}
	return nil
}

func handleStorageStatus(writer http.ResponseWriter, request *http.Request, mount *chaosMount, controller *chaosController) {
	if request.URL.Path != "/chaos/storage" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodGet {
		writeControlJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if mount == nil {
		writeControlJSON(writer, http.StatusConflict, map[string]string{"error": "chaosfs is not mounted"})
		return
	}
	writeControlJSON(writer, http.StatusOK, controller.status())
}

func handleStorageMode(writer http.ResponseWriter, request *http.Request, mount *chaosMount, controller *chaosController) {
	if request.URL.Path != "/chaos/storage/mode" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writeControlJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if mount == nil {
		writeControlJSON(writer, http.StatusConflict, map[string]string{"error": "chaosfs is not mounted"})
		return
	}
	if err := controller.setMode(request.URL.Query().Get("mode")); err != nil {
		writeControlJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeControlJSON(writer, http.StatusOK, controller.status())
}

func handlePowerCut(writer http.ResponseWriter, request *http.Request, mount *chaosMount, powerRequests chan<- powerRequest) {
	if request.URL.Path != "/chaos/power-cut" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writeControlJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if mount == nil {
		writeControlJSON(writer, http.StatusConflict, map[string]string{"error": "chaosfs is not mounted"})
		return
	}
	torn := false
	if values, exists := request.URL.Query()["torn"]; exists {
		if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			writeControlJSON(writer, http.StatusBadRequest, map[string]string{"error": "torn must be true or false"})
			return
		}
		torn = values[0] == "true"
	}
	submitPowerRequest(writer, request, powerRequests, "cut", torn)
}

func handlePowerOn(writer http.ResponseWriter, request *http.Request, mount *chaosMount, powerRequests chan<- powerRequest) {
	if request.URL.Path != "/chaos/power-on" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writeControlJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if mount == nil {
		writeControlJSON(writer, http.StatusConflict, map[string]string{"error": "chaosfs is not mounted"})
		return
	}
	submitPowerRequest(writer, request, powerRequests, "on", false)
}

func submitPowerRequest(writer http.ResponseWriter, request *http.Request, powerRequests chan<- powerRequest, kind string, torn bool) {
	reply := make(chan powerReply, 1)
	power := powerRequest{kind: kind, torn: torn, reply: reply}
	select {
	case powerRequests <- power:
	case <-request.Context().Done():
		return
	}
	select {
	case result := <-reply:
		writeControlJSON(writer, result.status, result.body)
	case <-request.Context().Done():
	}
}

func writeControlJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func processPowerRequest(request powerRequest, controller *chaosController, mount *chaosMount, broker, brokerConfig string, brokerCommand **exec.Cmd, brokerExit *<-chan error) error {
	if mount == nil {
		request.reply <- powerReply{status: http.StatusConflict, body: map[string]string{"error": "chaosfs is not mounted"}}
		return nil
	}
	switch request.kind {
	case "cut":
		if controller.status().PoweredOff {
			request.reply <- powerReply{status: http.StatusConflict, body: map[string]string{"error": "node is already powered off"}}
			return nil
		}
		controller.beginCut()
		var signalErr error
		if *brokerCommand != nil {
			if err := (*brokerCommand).Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
				signalErr = err
			}
		}
		released := controller.releaseHeld()
		if signalErr != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": signalErr.Error()}}
			return fmt.Errorf("kill broker for power cut: %w", signalErr)
		}

		exitCode := 0
		if *brokerCommand != nil {
			timer := time.NewTimer(10 * time.Second)
			var exitErr error
			select {
			case exitErr = <-*brokerExit:
			case <-timer.C:
				request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": "broker did not exit after power cut"}}
				return errors.New("broker did not exit after power cut")
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			exitCode = processExitCode(exitErr)
		}
		*brokerCommand = nil
		*brokerExit = nil

		result, err := mount.unmountAndRollback(request.torn)
		if err != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": err.Error()}}
			return err
		}
		result.BrokerExitCode = exitCode
		result.ReleasedFsyncs = released
		if err := mount.remount(); err != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": err.Error()}}
			return fmt.Errorf("remount chaos filesystem after power cut: %w", err)
		}
		controller.setPoweredOff(true)
		if err := writeJSON(map[string]any{"event": "power-cut", "torn": request.torn, "brokerExitCode": exitCode}); err != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": err.Error()}}
			return err
		}
		request.reply <- powerReply{status: http.StatusOK, body: result}
		return nil
	case "on":
		if !controller.status().PoweredOff {
			request.reply <- powerReply{status: http.StatusConflict, body: map[string]string{"error": "node is not powered off"}}
			return nil
		}
		command, exit, err := startBroker(broker, brokerConfig)
		if err != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": err.Error()}}
			return err
		}
		*brokerCommand = command
		*brokerExit = exit
		controller.setPoweredOff(false)
		if err := writeJSON(map[string]string{"event": "power-on"}); err != nil {
			request.reply <- powerReply{status: http.StatusInternalServerError, body: map[string]string{"error": err.Error()}}
			return err
		}
		request.reply <- powerReply{status: http.StatusOK, body: map[string]bool{"poweredOff": false}}
		return nil
	default:
		request.reply <- powerReply{status: http.StatusBadRequest, body: map[string]string{"error": "unknown power request"}}
		return nil
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
