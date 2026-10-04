package main

import (
	"flag"
	"fmt"
	"net/http"
)

func runJWKSCLI(args []string) error {
	flags := flag.NewFlagSet("jwks", flag.ContinueOnError)
	file := flags.String("file", "", "JWKS file")
	listen := flags.String("listen", "", "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *file == "" || *listen == "" {
		return fmt.Errorf("--file and --listen are required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		http.ServeFile(w, r, *file)
	})
	return http.ListenAndServe(*listen, mux)
}
