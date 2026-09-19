package mqtt

import "encoding/binary"

// Connect is the validated domain representation of an MQTT CONNECT packet.
type Connect struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// CleanStart requests a new session instead of resuming an old one.
	CleanStart bool
	// SessionExpiryInterval controls MQTT 5 session retention after disconnect.
	SessionExpiryInterval uint32
	// KeepAlive is the connection keep-alive interval in seconds.
	KeepAlive uint16
	// ClientID identifies the connecting client.
	ClientID string
	// HasUsername records whether the username field was present.
	HasUsername bool
	// Username is the optional UTF-8 username.
	Username string
	// HasPassword records whether the password field was present.
	HasPassword bool
	// Password is the optional binary password.
	Password []byte
	// Will is the optional last-will message.
	Will *Will
}

// Will is the last-will message carried by CONNECT.
type Will struct {
	// QoS is the will delivery level and may be 0, 1, or 2.
	QoS byte
	// Retain requests retained delivery of the will message.
	Retain bool
	// Topic is the non-empty UTF-8 will topic.
	Topic string
	// Payload is the binary will payload.
	Payload []byte
}

func (c Connect) packetType() PacketType {
	return PacketTypeConnect
}

func (c Connect) packetVersion() ProtocolVersion {
	return c.Version
}

func decodeConnect(body []byte, requestedVersion ProtocolVersion) (Packet, error) {
	position := 0
	protocolNameBytes, err := readBinaryField(body, &position)
	if err != nil || !validMQTTUTF8(string(protocolNameBytes)) {
		return nil, ErrMalformedPacket
	}
	if position >= len(body) {
		return nil, ErrMalformedPacket
	}
	level := body[position]
	position++

	version, err := connectProtocolVersion(string(protocolNameBytes), level)
	if err != nil {
		return nil, err
	}
	if requestedVersion != ProtocolUnknown && requestedVersion != version {
		return nil, ErrMalformedPacket
	}
	if len(body)-position < 3 {
		return nil, ErrMalformedPacket
	}

	flags := body[position]
	position++
	if flags&0x01 != 0 {
		return nil, ErrMalformedPacket
	}
	cleanStart := flags&0x02 != 0
	willFlag := flags&0x04 != 0
	willQoS := (flags >> 3) & 0x03
	willRetain := flags&0x20 != 0
	hasPassword := flags&0x40 != 0
	hasUsername := flags&0x80 != 0
	if willQoS == 3 || (!willFlag && (willQoS != 0 || willRetain)) {
		return nil, ErrMalformedPacket
	}
	if hasPassword && !hasUsername {
		return nil, ErrMalformedPacket
	}

	keepAlive := binary.BigEndian.Uint16(body[position : position+2])
	position += 2
	var sessionExpiryInterval uint32
	if version == ProtocolV5 {
		sessionExpiryInterval, err = decodeConnectProperties(body, &position)
		if err != nil {
			return nil, err
		}
	}

	clientID, err := readUTF8Field(body, &position, true)
	if err != nil {
		return nil, err
	}
	if version == ProtocolV311 && clientID == "" && !cleanStart {
		return nil, ErrMalformedPacket
	}

	var will *Will
	if willFlag {
		if version == ProtocolV5 {
			if err := skipProperties(body, &position); err != nil {
				return nil, err
			}
		}
		willTopic, err := readUTF8Field(body, &position, false)
		if err != nil || !validMQTTTopicName(willTopic) {
			return nil, ErrMalformedPacket
		}
		willPayload, err := readBinaryField(body, &position)
		if err != nil {
			return nil, err
		}
		will = &Will{
			QoS:     willQoS,
			Retain:  willRetain,
			Topic:   willTopic,
			Payload: cloneBytes(willPayload),
		}
	}

	username := ""
	if hasUsername {
		username, err = readUTF8Field(body, &position, true)
		if err != nil {
			return nil, err
		}
	}
	var password []byte
	if hasPassword {
		password, err = readBinaryField(body, &position)
		if err != nil {
			return nil, err
		}
		password = cloneBytes(password)
	}
	if position != len(body) {
		return nil, ErrMalformedPacket
	}

	return &Connect{
		Version:               version,
		CleanStart:            cleanStart,
		SessionExpiryInterval: sessionExpiryInterval,
		KeepAlive:             keepAlive,
		ClientID:              clientID,
		HasUsername:           hasUsername,
		Username:              username,
		HasPassword:           hasPassword,
		Password:              password,
		Will:                  will,
	}, nil
}

func connectProtocolVersion(protocolName string, level byte) (ProtocolVersion, error) {
	switch protocolName {
	case "MQTT":
		switch level {
		case byte(ProtocolV311):
			return ProtocolV311, nil
		case byte(ProtocolV5):
			return ProtocolV5, nil
		default:
			return ProtocolUnknown, &UnsupportedProtocolVersionError{ProtocolName: protocolName, Level: level}
		}
	case "MQIsdp":
		if level == 3 {
			return ProtocolUnknown, &UnsupportedProtocolVersionError{ProtocolName: protocolName, Level: level}
		}
	}
	return ProtocolUnknown, ErrMalformedPacket
}

func encodeConnect(connect Connect, maxPacketSize int) ([]byte, error) {
	bodySize, err := validateConnect(connect)
	if err != nil {
		return nil, err
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}

	body := make([]byte, 0, bodySize)
	body = append(body, 0, 4, 'M', 'Q', 'T', 'T')
	body = append(body, byte(connect.Version))
	flags := byte(0)
	if connect.CleanStart {
		flags |= 0x02
	}
	if connect.Will != nil {
		flags |= 0x04 | (connect.Will.QoS << 3)
		if connect.Will.Retain {
			flags |= 0x20
		}
	}
	if connect.HasUsername {
		flags |= 0x80
	}
	if connect.HasPassword {
		flags |= 0x40
	}
	body = append(body, flags)
	var keepAlive [2]byte
	binary.BigEndian.PutUint16(keepAlive[:], connect.KeepAlive)
	body = append(body, keepAlive[:]...)
	if connect.Version == ProtocolV5 {
		if connect.SessionExpiryInterval == 0 {
			body = append(body, 0)
		} else {
			body = append(body, 5, 0x11)
			var sessionExpiryInterval [4]byte
			binary.BigEndian.PutUint32(sessionExpiryInterval[:], connect.SessionExpiryInterval)
			body = append(body, sessionExpiryInterval[:]...)
		}
	}
	body = appendUTF8Field(body, connect.ClientID)
	if connect.Will != nil {
		if connect.Version == ProtocolV5 {
			body = append(body, 0)
		}
		body = appendUTF8Field(body, connect.Will.Topic)
		body = appendBinaryField(body, connect.Will.Payload)
	}
	if connect.HasUsername {
		body = appendUTF8Field(body, connect.Username)
	}
	if connect.HasPassword {
		body = appendBinaryField(body, connect.Password)
	}
	return body, nil
}

func validateConnect(connect Connect) (int, error) {
	if connect.Version != ProtocolV311 && connect.Version != ProtocolV5 {
		return 0, ErrUnsupportedProtocolVersion
	}
	if connect.Version == ProtocolV311 && connect.SessionExpiryInterval != 0 {
		return 0, ErrMalformedPacket
	}
	if !validMQTTUTF8(connect.ClientID) || len(connect.ClientID) > 65535 {
		return 0, ErrMalformedPacket
	}
	if connect.Version == ProtocolV311 && connect.ClientID == "" && !connect.CleanStart {
		return 0, ErrMalformedPacket
	}
	if connect.HasUsername {
		if !validMQTTUTF8(connect.Username) || len(connect.Username) > 65535 {
			return 0, ErrMalformedPacket
		}
	} else if connect.Username != "" {
		return 0, ErrMalformedPacket
	}
	if connect.HasPassword {
		if len(connect.Password) > 65535 {
			return 0, ErrMalformedPacket
		}
	} else if len(connect.Password) != 0 {
		return 0, ErrMalformedPacket
	}
	if connect.HasPassword && !connect.HasUsername {
		return 0, ErrMalformedPacket
	}
	if connect.Will != nil {
		if connect.Will.QoS > 2 || !validMQTTTopicName(connect.Will.Topic) || len(connect.Will.Topic) > 65535 || len(connect.Will.Payload) > 65535 {
			return 0, ErrMalformedPacket
		}
	}

	bodySize := 10 + 2 + len(connect.ClientID)
	if connect.Version == ProtocolV5 {
		bodySize++
		if connect.SessionExpiryInterval != 0 {
			bodySize += 5
		}
	}
	if connect.Will != nil {
		bodySize += 2 + len(connect.Will.Topic) + 2 + len(connect.Will.Payload)
		if connect.Version == ProtocolV5 {
			bodySize++
		}
	}
	if connect.HasUsername {
		bodySize += 2 + len(connect.Username)
	}
	if connect.HasPassword {
		bodySize += 2 + len(connect.Password)
	}
	return bodySize, nil
}

func readBinaryField(body []byte, position *int) ([]byte, error) {
	if len(body)-*position < 2 {
		return nil, ErrMalformedPacket
	}
	length := int(binary.BigEndian.Uint16(body[*position : *position+2]))
	*position += 2
	if length > len(body)-*position {
		return nil, ErrMalformedPacket
	}
	field := body[*position : *position+length]
	*position += length
	return field, nil
}

func readUTF8Field(body []byte, position *int, allowEmpty bool) (string, error) {
	field, err := readBinaryField(body, position)
	if err != nil {
		return "", err
	}
	value := string(field)
	if (!allowEmpty && len(value) == 0) || !validMQTTUTF8(value) {
		return "", ErrMalformedPacket
	}
	return value, nil
}

func skipProperties(body []byte, position *int) error {
	length, consumed, err := decodeVariableByteInteger(body[*position:])
	if err != nil {
		return ErrMalformedPacket
	}
	*position += consumed
	if length > len(body)-*position {
		return ErrMalformedPacket
	}
	if length != 0 {
		*position += length
		return ErrUnsupportedProperty
	}
	return nil
}
func decodeConnectProperties(body []byte, position *int) (uint32, error) {
	length, consumed, err := decodeVariableByteInteger(body[*position:])
	if err != nil {
		return 0, ErrMalformedPacket
	}
	*position += consumed
	if length > len(body)-*position {
		return 0, ErrMalformedPacket
	}
	end := *position + length
	var (
		sessionExpiryInterval uint32
		seenSessionExpiry     bool
	)
	for *position < end {
		propertyIdentifier := body[*position]
		*position++
		switch propertyIdentifier {
		case 0x11:
			if seenSessionExpiry || end-*position < 4 {
				return 0, ErrMalformedPacket
			}
			sessionExpiryInterval = binary.BigEndian.Uint32(body[*position : *position+4])
			*position += 4
			seenSessionExpiry = true
		default:
			return 0, ErrUnsupportedProperty
		}
	}
	return sessionExpiryInterval, nil
}

func appendUTF8Field(dst []byte, value string) []byte {
	length := len(value)
	dst = append(dst, byte(length>>8), byte(length))
	return append(dst, value...)
}

func appendBinaryField(dst []byte, value []byte) []byte {
	length := len(value)
	dst = append(dst, byte(length>>8), byte(length))
	return append(dst, value...)
}
