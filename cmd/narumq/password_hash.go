package main

import (
	"errors"
	"io"

	authenticationadapter "narumq/internal/adapters/authentication"
)

const maxPasswordHashInputBytes = 65538

// hashPasswordCommand reads a password from stdin and writes its Argon2id hash.
func hashPasswordCommand(reader io.Reader, writer io.Writer) error {
	if reader == nil || writer == nil {
		return errors.New("password hash input or output is nil")
	}
	password, err := io.ReadAll(io.LimitReader(reader, maxPasswordHashInputBytes))
	if err != nil {
		clear(password)
		return err
	}
	defer clear(password)

	if len(password) > 0 && password[len(password)-1] == '\n' {
		password = password[:len(password)-1]
		if len(password) > 0 && password[len(password)-1] == '\r' {
			password = password[:len(password)-1]
		}
	}
	if len(password) == 0 || len(password) > 65535 {
		return authenticationadapter.ErrInvalidPasswordLength
	}

	encoded, err := authenticationadapter.HashPassword(password)
	if err != nil {
		return err
	}
	output := make([]byte, len(encoded)+1)
	copy(output, encoded)
	output[len(output)-1] = '\n'
	written, err := writer.Write(output)
	if err != nil {
		return err
	}
	if written != len(output) {
		return io.ErrShortWrite
	}
	return nil
}
