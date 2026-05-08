package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const magicByte byte = 0x0

func SerializeUsingPayloadPrefix(id uint32, payload []byte) ([]byte, error) {

	var buf bytes.Buffer
	err := buf.WriteByte(magicByte)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(payload)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeUsingPayloadPrefix(payload []byte) (uint32, []byte, error) {
	if payload[0] != magicByte {
		return 0, nil, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	return id, payload[5:], nil
}
