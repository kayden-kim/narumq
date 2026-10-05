package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "bp07: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("subcommand is required: run, compare, suite, or serve-jwks")
	}
	switch args[0] {
	case "run":
		return runCommand(args[1:], stdout, stderr)
	case "compare":
		return compareCommand(context.Background(), args[1:], stdout, stderr)
	case "suite":
		return suiteCommand(context.Background(), args[1:], stdout, stderr)
	case "serve-jwks":
		return serveJWKSCommand(args[1:], stderr)
	default:
		return fmt.Errorf("unknown subcommand %q; only run, compare, suite, and serve-jwks are supported", args[0])
	}
}

func serveJWKSCommand(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("serve-jwks", flag.ContinueOnError)
	flags.SetOutput(stderr)
	filePath := flags.String("file", "", "path to the JWKS JSON file")
	listenAddress := flags.String("listen", "", "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected serve-jwks arguments: %v", flags.Args())
	}
	if strings.TrimSpace(*filePath) == "" || strings.TrimSpace(*listenAddress) == "" {
		return errors.New("serve-jwks requires --file and --listen")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveJWKS(ctx, *filePath, *listenAddress)
}

func serveJWKS(ctx context.Context, filePath, listenAddress string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(filePath) == "" || strings.TrimSpace(listenAddress) == "" {
		return errors.New("JWKS file and listen address are required")
	}
	server := &http.Server{
		Addr:              listenAddress,
		Handler:           jwksHandler(filePath),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve JWKS: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down JWKS server: %w", err)
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve JWKS: %w", err)
		}
		return nil
	}
}

func jwksHandler(filePath string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		http.ServeFile(writer, request, filePath)
	})
	return mux
}
