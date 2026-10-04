package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func malformedDisconnect(vector string) ([]byte, error) {
	const trap = "6c6f63616c686f73743a31383836"
	vectors := map[string]string{
		"truncated_property":              "e0079c051c00ff7472",
		"disallowed_property":             "e0169c142300011c000e" + trap,
		"duplicate_server_reference":      "e0249c221c000e" + trap + "1c000e" + trap,
		"invalid_server_reference_format": "e01a9c181c00156d7174743a2f2f" + trap,
	}
	encoded, ok := vectors[vector]
	if !ok {
		return nil, fmt.Errorf("unsupported vector %q", vector)
	}
	return hex.DecodeString(encoded)
}

func fakeReadVBI(reader io.ByteReader) (int, error) {
	value, multiplier := 0, 1
	for count := 0; count < 4; count++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&127) * multiplier
		if b&128 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("invalid MQTT remaining length")
}
func fakeReadString(reader *bufio.Reader) (string, error) {
	high, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	low, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	value := make([]byte, int(high)*256+int(low))
	_, err = io.ReadFull(reader, value)
	return string(value), err
}
func fakeConnect(reader *bufio.Reader) (string, error) {
	header, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	if header != 0x10 {
		return "", fmt.Errorf("expected CONNECT")
	}
	length, err := fakeReadVBI(reader)
	if err != nil {
		return "", err
	}
	if length > 65536 {
		return "", fmt.Errorf("CONNECT too large")
	}
	limited := &io.LimitedReader{R: reader, N: int64(length)}
	packet := bufio.NewReader(limited)
	protocol, err := fakeReadString(packet)
	if err != nil {
		return "", err
	}
	if protocol != "MQTT" {
		return "", fmt.Errorf("invalid protocol")
	}
	level, err := packet.ReadByte()
	if err != nil {
		return "", err
	}
	if level != 5 {
		return "", fmt.Errorf("expected MQTT 5")
	}
	if _, err := io.CopyN(io.Discard, packet, 3); err != nil {
		return "", err
	}
	properties, err := fakeReadVBI(packet)
	if err != nil {
		return "", err
	}
	if _, err := io.CopyN(io.Discard, packet, int64(properties)); err != nil {
		return "", err
	}
	clientID, err := fakeReadString(packet)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(io.Discard, packet); err != nil {
		return "", err
	}
	if limited.N != 0 {
		return "", io.ErrUnexpectedEOF
	}
	return clientID, nil
}

func runFakeServerCLI(args []string) error {
	flags := flag.NewFlagSet("fakeserver", flag.ContinueOnError)
	listen := flags.String("listen", "", "configured endpoint")
	trap := flags.String("trap", "", "forbidden endpoint")
	vector := flags.String("vector", "", "malformed DISCONNECT vector")
	tracePath := flags.String("trace", "", "JSONL trace output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *listen == "" || *trap == "" || *tracePath == "" {
		return fmt.Errorf("--listen, --trap, --vector and --trace are required")
	}
	payload, err := malformedDisconnect(*vector)
	if err != nil {
		return err
	}
	mainListener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer mainListener.Close()
	trapListener, err := net.Listen("tcp", *trap)
	if err != nil {
		return err
	}
	defer trapListener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	trace := &helperTrace{path: *tracePath}
	trace.mustRecord(map[string]any{"event": "listening", "listener": "main"})
	trace.mustRecord(map[string]any{"event": "listening", "listener": "trap"})
	go func() { <-ctx.Done(); _ = mainListener.Close(); _ = trapListener.Close() }()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := trapListener.Accept()
			if err != nil {
				return
			}
			trace.mustRecord(map[string]any{"event": "trap_accept", "listener": "trap"})
			_ = conn.Close()
		}
	}()
	defer workers.Wait()
	defer stop()
	connection := 0
	for {
		conn, err := mainListener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		connection++
		number := connection
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer conn.Close()
			done := make(chan struct{})
			defer close(done)
			go func() {
				select {
				case <-ctx.Done():
					_ = conn.Close()
				case <-done:
				}
			}()
			_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
			reader := bufio.NewReader(conn)
			clientID, err := fakeConnect(reader)
			if err != nil {
				trace.mustRecord(map[string]any{"event": "connect_invalid", "listener": "main", "connection": number})
				return
			}
			trace.mustRecord(map[string]any{"event": "connect", "listener": "main", "connection": number, "clientId": clientID})
			if _, err := conn.Write([]byte{0x20, 3, 0, 0, 0}); err != nil {
				return
			}
			if number == 1 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(300 * time.Millisecond):
				}
				if _, err := conn.Write(payload); err != nil {
					return
				}
				trace.mustRecord(map[string]any{"event": "vector_sent", "listener": "main", "connection": number, "vector": *vector})
				return
			}
			for {
				header, err := reader.ReadByte()
				if err != nil {
					trace.mustRecord(map[string]any{"event": "disconnect", "listener": "main", "connection": number})
					return
				}
				length, err := fakeReadVBI(reader)
				if err != nil {
					return
				}
				if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
					return
				}
				if header == 0xc0 {
					if _, err := conn.Write([]byte{0xd0, 0}); err != nil {
						return
					}
				}
				if header>>4 == 14 {
					trace.mustRecord(map[string]any{"event": "disconnect", "listener": "main", "connection": number})
					return
				}
			}
		}()
	}
}
