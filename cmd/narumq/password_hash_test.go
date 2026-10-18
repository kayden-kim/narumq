package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	authenticationadapter "narumq/internal/adapters/authentication"
	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestHashPasswordCommandTrimsOnlyOneLineEndingAndWritesVerifiableHash(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		password []byte
	}{
		{name: "one LF", input: []byte("password\n"), password: []byte("password")},
		{name: "one CRLF", input: []byte("password\r\n"), password: []byte("password")},
		{name: "preserve whitespace", input: []byte(" password \n"), password: []byte(" password ")},
		{name: "preserve binary bytes", input: []byte{0x00, 'p', 0xff, '\r'}, password: []byte{0x00, 'p', 0xff, '\r'}},
		{name: "remove one LF only", input: []byte("password\n\n"), password: []byte("password\n")},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := hashPasswordCommand(bytes.NewReader(testCase.input), &output); err != nil {
				t.Fatal(err)
			}
			encoded := output.Bytes()
			if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' || bytes.Count(encoded, []byte{'\n'}) != 1 {
				t.Fatalf("hash output = %q, want exactly one terminating LF", encoded)
			}
			phc := string(encoded[:len(encoded)-1])
			passwordHash, err := authenticationadapter.ParsePasswordHash(phc)
			if err != nil {
				t.Fatalf("command output is not a valid PHC hash: %v", err)
			}
			tenantID, err := tenancy.NewTenantID("tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			provider, err := authenticationadapter.NewPasswordAuthenticationProvider([]authenticationadapter.PasswordIdentityBinding{{
				Username:         "device",
				PasswordHash:     passwordHash,
				TenantID:         tenantID,
				Subject:          "device-subject",
				CredentialID:     "device-credential",
				AllowedClientIDs: []string{"device-client"},
				Roles:            []security.Role{"publisher"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "device", Password: testCase.password})
			if err != nil || result.Subject != "device-subject" {
				t.Fatalf("command hash did not verify input bytes: result=%#v err=%v", result, err)
			}
		})
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestHashPasswordCommandRejectsInvalidInputWithoutWriting(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
	}{
		{name: "empty"},
		{name: "one LF only", input: []byte("\n")},
		{name: "oversized", input: bytes.Repeat([]byte{'x'}, 65536)},
		{name: "oversized after line ending", input: append(bytes.Repeat([]byte{'x'}, 65536), '\n')},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var output bytes.Buffer
			err := hashPasswordCommand(bytes.NewReader(testCase.input), &output)
			if err == nil || err.Error() != "password must contain between 1 and 65535 bytes" {
				t.Fatalf("hashPasswordCommand() error = %v, want password byte range error", err)
			}
			if output.Len() != 0 {
				t.Fatalf("invalid input wrote %d stdout bytes", output.Len())
			}
		})
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestHashPasswordCommandReturnsReaderAndWriterErrors(t *testing.T) {
	readerErr := errors.New("reader failed")
	var output bytes.Buffer
	if err := hashPasswordCommand(passwordHashErrorReader{err: readerErr}, &output); !errors.Is(err, readerErr) {
		t.Fatalf("reader error = %v, want %v", err, readerErr)
	}
	if output.Len() != 0 {
		t.Fatalf("reader failure wrote %d stdout bytes", output.Len())
	}

	password := []byte("writer-boundary-password")
	writerErr := errors.New("writer failed")
	if err := hashPasswordCommand(bytes.NewReader(password), passwordHashErrorWriter{err: writerErr}); !errors.Is(err, writerErr) {
		t.Fatalf("writer error = %v, want %v", err, writerErr)
	}
	if err := hashPasswordCommand(bytes.NewReader(password), passwordHashShortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short writer error = %v, want %v", err, io.ErrShortWrite)
	}
}

type passwordHashErrorReader struct {
	err error
}

func (reader passwordHashErrorReader) Read([]byte) (int, error) {
	return 0, reader.err
}

type passwordHashErrorWriter struct {
	err error
}

func (writer passwordHashErrorWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

type passwordHashShortWriter struct{}

func (passwordHashShortWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}
