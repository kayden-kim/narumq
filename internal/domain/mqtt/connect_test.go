package mqtt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestConnectRoundTripAndOwnership_MQTT311_MQTT312_MQTT313_MQTT320(t *testing.T) {
	cases := []struct {
		name  string
		input Connect
		trace string
	}{
		{
			name: "v311-clean-session",
			input: Connect{
				Version:    ProtocolV311,
				CleanStart: true,
				KeepAlive:  30,
				ClientID:   "client-311",
			},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.1.2-1/2/3/11/13/14/15/18/19/20/21/22; MQTT-3.1.3-1/3/4/10/11",
		},
		{
			name: "v311-all-presence-combinations",
			input: Connect{
				Version:     ProtocolV311,
				CleanStart:  true,
				KeepAlive:   120,
				ClientID:    "client-optional",
				HasUsername: true,
				Username:    "",
				HasPassword: true,
				Password:    []byte{},
				Will: &Will{
					QoS:     2,
					Retain:  true,
					Topic:   "will/topic",
					Payload: []byte{0x00, 0xFF},
				},
			},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.1.2-13/14/15/18/19/20/21/22; MQTT-3.1.3-1/3/4/10/11",
		},
		{
			name: "v5-property-free",
			input: Connect{
				Version:     ProtocolV5,
				CleanStart:  true,
				KeepAlive:   45,
				ClientID:    "client-5",
				HasUsername: true,
				Username:    "user",
				HasPassword: true,
				Password:    []byte{0x00, 0xFE},
				Will: &Will{
					QoS:     0,
					Topic:   "will/5",
					Payload: []byte("binary\x00payload"),
				},
			},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.1.2-1/2/3/11/12/13/16/17/18/19; MQTT-3.1.3-1/3/4/11/12",
		},
		{
			name: "v5-zero-client-id-without-clean-start",
			input: Connect{
				Version:    ProtocolV5,
				CleanStart: false,
				ClientID:   "",
			},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.1.3-1/3/4/11/12",
		},
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			wire := testEncode(t, encoder, &testCase.input)
			decoded := testDecode(t, decoder, wire, ProtocolUnknown)
			requireSemanticEqual(t, &testCase.input, decoded)

			beforeMutation := packetValue(decoded)
			for index := range wire {
				wire[index] = 0xA5
			}
			if !reflect.DeepEqual(beforeMutation, packetValue(decoded)) {
				t.Fatalf("decoded CONNECT changed after source mutation (%s)", testCase.trace)
			}
		})
	}
}

func TestConnectCanonicalWire_MQTT311_MQTT5(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	v311 := testEncode(t, encoder, Connect{Version: ProtocolV311, CleanStart: true, ClientID: "a"})
	wantV311 := []byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x00, 0x00, 0x01, 'a'}
	if !bytes.Equal(v311, wantV311) {
		t.Fatalf("MQTT 3.1.1 wire = %v, want %v", v311, wantV311)
	}
	v5 := testEncode(t, encoder, Connect{Version: ProtocolV5, CleanStart: true, ClientID: "a"})
	wantV5 := []byte{0x10, 0x0E, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 'a'}
	if !bytes.Equal(v5, wantV5) {
		t.Fatalf("MQTT 5 wire = %v, want %v", v5, wantV5)
	}
}

func TestConnectRejectsMalformedFields_MQTT311_2_2_2_1_MQTT5_1_5_5_1(t *testing.T) {
	validBody := func() []byte {
		return []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x00, 0x00, 0x01, 'a'}
	}
	cases := []struct {
		name    string
		frame   []byte
		version ProtocolVersion
		want    error
		trace   string
	}{
		{
			name:    "reserved-fixed-header",
			frame:   testFrame(0x11, validBody()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-2.2.2-1/2",
		},
		{
			name:    "reserved-connect-flag",
			frame:   testFrame(0x10, func() []byte { body := validBody(); body[7] = 0x03; return body }()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.1.2-3",
		},
		{
			name:    "password-without-username",
			frame:   testFrame(0x10, func() []byte { body := validBody(); body[7] = 0x42; return body }()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.1.2-22",
		},
		{
			name:    "truncated-length-field",
			frame:   testFrame(0x10, []byte{0x00, 0x04, 'M'}),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.1.3-1",
		},
		{
			name:    "trailing-byte",
			frame:   testFrame(0x10, append(validBody(), 0x00)),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.1.3-11",
		},
		{
			name: "invalid-utf8-client-id",
			frame: testFrame(0x10, func() []byte {
				body := validBody()
				body[len(body)-1] = 0xC0
				body = append(body, 0x80)
				body[11] = 0x00
				body[12] = 0x02
				return body
			}()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-1.5.3-1/2",
		},
		{
			name: "nul-client-id",
			frame: testFrame(0x10, func() []byte {
				body := validBody()
				body[len(body)-1] = 0
				return body
			}()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-1.5.3-1/2",
		},
		{
			name: "v5-nonzero-properties",
			frame: testFrame(0x10, func() []byte {
				return []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x00, 0x01, 0x00}
			}()),
			version: ProtocolUnknown,
			want:    ErrUnsupportedProperty,
			trace:   "PRD MQTT-01/TEST-04; PRD MQTT-12/TEST-04; MQTT-1.5.5-1",
		},
		{
			name: "v5-truncated-properties",
			frame: testFrame(0x10, func() []byte {
				return []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x00, 0x01}
			}()),
			version: ProtocolUnknown,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-04; PRD MQTT-12/TEST-04; MQTT-1.5.5-1",
		},
		{
			name:    "known-version-mismatch",
			frame:   testFrame(0x10, validBody()),
			version: ProtocolV5,
			want:    ErrMalformedPacket,
			trace:   "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.1.2-1",
		},
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, testCase.version)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Decode() error = %v, want %v (%s)", err, testCase.want, testCase.trace)
			}
			if consumed != 0 {
				t.Fatalf("Decode() consumed %d bytes on error", consumed)
			}
		})
	}
}

// Traceability: PRD MQTT-13/TEST-13; OASIS MQTT-3.1.2-1/2/3 and MQTT-3.1.3-1.
func TestConnectRejectsUnsupportedProtocolVersions_MQTT312_MQTT313(t *testing.T) {
	cases := []struct {
		name         string
		protocolName string
		level        byte
	}{
		{name: "MQIsdp-level-3", protocolName: "MQIsdp", level: 3},
		{name: "MQTT-level-3", protocolName: "MQTT", level: 3},
		{name: "MQTT-unknown-level", protocolName: "MQTT", level: 9},
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			body := []byte{0, byte(len(testCase.protocolName))}
			body = append(body, testCase.protocolName...)
			body = append(body, testCase.level, 0x02, 0x00, 0x00, 0x00, 0x01, 'a')
			_, _, err := decoder.Decode(testFrame(0x10, body), ProtocolUnknown)
			var typed UnsupportedProtocolVersionError
			if !errors.As(err, &typed) || !errors.Is(err, ErrUnsupportedProtocolVersion) {
				t.Fatalf("error = %v, want typed unsupported version", err)
			}
			if typed.ProtocolName != testCase.protocolName || typed.Level != testCase.level {
				t.Fatalf("typed error = %#v, want protocol %q level %d", typed, testCase.protocolName, testCase.level)
			}
		})
	}
}

func TestConnectEncoderLeavesDestinationUnchangedOnError_MQTT313(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	destination := []byte{0xAA, 0xBB, 0xCC}
	before := append([]byte(nil), destination...)
	_, err = encoder.Append(destination, Connect{Version: ProtocolV311, CleanStart: true, ClientID: "bad\x00client"})
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("Append() error = %v, want ErrMalformedPacket", err)
	}
	if !bytes.Equal(destination, before) {
		t.Fatalf("destination changed after encoder error: %v -> %v", before, destination)
	}
}
