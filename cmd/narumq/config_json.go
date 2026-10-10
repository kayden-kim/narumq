package main

import (
	"bytes"
	"encoding/json"
	"errors"
)

// errDuplicateJSONKey reports that one JSON object repeats an exact member name.
var errDuplicateJSONKey = errors.New("duplicate JSON key")

// rejectDuplicateJSONKeys walks one JSON value and returns errDuplicateJSONKey when any
// object repeats an exact, case-sensitive member name. Other errors report malformed JSON.
func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var visit func() error
	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		seen := make(map[string]struct{})
		for decoder.More() {
			if delimiter == '{' {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				// Decoder.Token returns only string tokens in object-key position.
				key := keyToken.(string)
				if _, exists := seen[key]; exists {
					return errDuplicateJSONKey
				}
				seen[key] = struct{}{}
			}
			if err := visit(); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	return visit()
}
