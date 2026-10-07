package auditlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type VerificationResultDTO struct {
	Files    int
	Records  uint64
	FirstSeq uint64
	LastSeq  uint64
	LastHash string
	TornTail bool
}

// VerifyDirectory validates audit JSONL records without modifying any file.
func VerifyDirectory(directory string) (VerificationResultDTO, error) {
	rotations, err := listAuditRotationFiles(directory)
	if err != nil {
		return VerificationResultDTO{}, err
	}
	state := auditChainState{hash: genesisHash}
	result := VerificationResultDTO{}
	for _, rotation := range rotations {
		data, err := os.ReadFile(rotation.path)
		if err != nil {
			return VerificationResultDTO{}, fmt.Errorf("read %s: %w", rotation.name, err)
		}
		records, err := scanAuditData(data, rotation.name, false)
		if err != nil {
			return VerificationResultDTO{}, err
		}
		if len(records) == 0 {
			return VerificationResultDTO{}, fmt.Errorf("%s:1: rotation file is empty", rotation.name)
		}
		state, err = verifyRecords(records, state, rotation.firstSeq, rotation.name)
		if err != nil {
			return VerificationResultDTO{}, err
		}
		result.Files++
	}

	activePath := filepath.Join(directory, activeFileName)
	if _, err := os.Lstat(activePath); err == nil {
		data, readErr := os.ReadFile(activePath)
		if readErr != nil {
			return VerificationResultDTO{}, fmt.Errorf("read %s: %w", activeFileName, readErr)
		}
		result.TornTail = len(data) > 0 && data[len(data)-1] != '\n'
		records, scanErr := scanAuditData(data, activeFileName, true)
		if scanErr != nil {
			return VerificationResultDTO{}, scanErr
		}
		state, err = verifyRecords(records, state, 0, activeFileName)
		if err != nil {
			return VerificationResultDTO{}, err
		}
		result.Files++
	} else if !errors.Is(err, os.ErrNotExist) {
		return VerificationResultDTO{}, fmt.Errorf("inspect %s: %w", activeFileName, err)
	}

	result.Records = state.records
	result.FirstSeq = state.firstSeq
	result.LastSeq = state.seq
	result.LastHash = state.hash
	if result.LastHash == "" {
		result.LastHash = genesisHash
	}
	return result, nil
}

func verifyRotationFiles(rotations []auditRotationFile) (auditChainState, error) {
	state := auditChainState{hash: genesisHash}
	for _, rotation := range rotations {
		data, err := os.ReadFile(rotation.path)
		if err != nil {
			return auditChainState{}, fmt.Errorf("read %s: %w", rotation.name, err)
		}
		records, err := scanAuditData(data, rotation.name, false)
		if err != nil {
			return auditChainState{}, err
		}
		if len(records) == 0 {
			return auditChainState{}, fmt.Errorf("%s:1: rotation file is empty", rotation.name)
		}
		state, err = verifyRecords(records, state, rotation.firstSeq, rotation.name)
		if err != nil {
			return auditChainState{}, err
		}
	}
	return state, nil
}

func verifyRecords(records []auditLogRecord, state auditChainState, expectedFirst uint64, fileName string) (auditChainState, error) {
	for index, record := range records {
		line := index + 1
		if index == 0 && expectedFirst > 0 && record.Seq != expectedFirst {
			return state, fmt.Errorf("%s:%d: first sequence %d does not match file name sequence %d", fileName, line, record.Seq, expectedFirst)
		}
		if !state.hasRecord {
			if record.Seq == 1 && record.PrevHash != genesisHash {
				return state, fmt.Errorf("%s:%d: first record does not use the genesis hash", fileName, line)
			}
			state.firstSeq = record.Seq
		} else {
			if state.seq == ^uint64(0) || record.Seq != state.seq+1 {
				return state, fmt.Errorf("%s:%d: sequence %d does not follow %d", fileName, line, record.Seq, state.seq)
			}
			if record.PrevHash != state.hash {
				return state, fmt.Errorf("%s:%d: previous hash does not match the preceding record", fileName, line)
			}
		}
		state.seq = record.Seq
		state.hash = record.Hash
		state.records++
		state.hasRecord = true
	}
	return state, nil
}
