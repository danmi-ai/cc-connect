package infoflow

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ─── Wire constants (protobuf-style) ──────────────────────────────────────────

const (
	wireVarint = 0
	wireLen    = 2
)

// Frame method constants mirroring Infoflow WS protocol.
const (
	FrameMethodControl  = 0
	FrameMethodData     = 1
	FrameMethodRequest  = 2
	FrameMethodResponse = 3
)

// Header is a key-value pair in a frame.
type Header struct {
	Key   string
	Value string
}

// Frame is the decoded wire-level structure of an Infoflow WS message.
type Frame struct {
	SeqID   int
	LogID   string
	Service int
	Method  int
	Headers []Header
	Payload []byte // raw JSON bytes
}

// ─── Varint helpers ───────────────────────────────────────────────────────────

func writeVarint(v int) []byte {
	if v == 0 {
		return []byte{0}
	}
	var out []byte
	for v > 0 {
		b := byte(v & 0x7f)
		v >>= 7
		if v > 0 {
			b |= 0x80
		}
		out = append(out, b)
	}
	return out
}

func readVarint(buf []byte, pos int) (int, int, error) {
	value := 0
	shift := 0
	for {
		if pos >= len(buf) {
			return 0, pos, errors.New("truncated varint")
		}
		b := buf[pos]
		pos++
		value |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, pos, nil
		}
		shift += 7
		if shift > 49 { // safety: max ~7 bytes for reasonable values
			return 0, pos, errors.New("varint overflow")
		}
	}
}

// ─── Encode helpers ───────────────────────────────────────────────────────────

func tagBytes(field, wire int) []byte {
	return writeVarint(field<<3 | wire)
}

func encodeVarintField(field, value int) []byte {
	out := tagBytes(field, wireVarint)
	out = append(out, writeVarint(value)...)
	return out
}

func encodeLenField(field int, data []byte) []byte {
	out := tagBytes(field, wireLen)
	out = append(out, writeVarint(len(data))...)
	out = append(out, data...)
	return out
}

func encodeStringField(field int, s string) []byte {
	if s == "" {
		return nil
	}
	return encodeLenField(field, []byte(s))
}

func encodeHeader(h Header) []byte {
	var out []byte
	out = append(out, encodeStringField(1, h.Key)...)
	out = append(out, encodeStringField(2, h.Value)...)
	return out
}

// EncodeFrame serializes a Frame to binary.
func EncodeFrame(f *Frame) []byte {
	var parts []byte
	parts = append(parts, encodeVarintField(1, f.SeqID)...)
	if f.LogID != "" {
		parts = append(parts, encodeStringField(2, f.LogID)...)
	}
	if f.Service != 0 {
		parts = append(parts, encodeVarintField(3, f.Service)...)
	}
	parts = append(parts, encodeVarintField(4, f.Method)...)
	for _, h := range f.Headers {
		encoded := encodeHeader(h)
		tag := tagBytes(5, wireLen)
		tag = append(tag, writeVarint(len(encoded))...)
		tag = append(tag, encoded...)
		parts = append(parts, tag...)
	}
	if len(f.Payload) > 0 {
		parts = append(parts, encodeLenField(6, f.Payload)...)
	}
	return parts
}

// DecodeFrame deserializes binary data into a Frame.
func DecodeFrame(raw []byte) (*Frame, error) {
	f := &Frame{}
	pos := 0
	for pos < len(raw) {
		tag, p, err := readVarint(raw, pos)
		if err != nil {
			return nil, fmt.Errorf("read tag: %w", err)
		}
		pos = p
		fieldNumber := tag >> 3
		wireType := tag & 7

		switch wireType {
		case wireVarint:
			val, p2, err := readVarint(raw, pos)
			if err != nil {
				return nil, fmt.Errorf("read varint field %d: %w", fieldNumber, err)
			}
			pos = p2
			switch fieldNumber {
			case 1:
				f.SeqID = val
			case 3:
				f.Service = val
			case 4:
				f.Method = val
			}

		case wireLen:
			length, p2, err := readVarint(raw, pos)
			if err != nil {
				return nil, fmt.Errorf("read len field %d: %w", fieldNumber, err)
			}
			pos = p2
			if pos+length > len(raw) {
				return nil, fmt.Errorf("field %d length %d exceeds buffer", fieldNumber, length)
			}
			data := raw[pos : pos+length]
			pos += length

			switch fieldNumber {
			case 2:
				f.LogID = string(data)
			case 5:
				h, err := decodeHeader(data)
				if err == nil {
					f.Headers = append(f.Headers, h)
				}
			case 6:
				f.Payload = make([]byte, len(data))
				copy(f.Payload, data)
			}

		default:
			return nil, fmt.Errorf("unsupported wire type %d at field %d", wireType, fieldNumber)
		}
	}
	return f, nil
}

func decodeHeader(data []byte) (Header, error) {
	var h Header
	pos := 0
	for pos < len(data) {
		tag, p, err := readVarint(data, pos)
		if err != nil {
			return h, err
		}
		pos = p
		fieldNumber := tag >> 3
		wireType := tag & 7

		if wireType == wireLen {
			length, p2, err := readVarint(data, pos)
			if err != nil {
				return h, err
			}
			pos = p2
			if pos+length > len(data) {
				return h, fmt.Errorf("header field %d length %d exceeds buffer", fieldNumber, length)
			}
			s := string(data[pos : pos+length])
			pos += length
			switch fieldNumber {
			case 1:
				h.Key = s
			case 2:
				h.Value = s
			}
		} else if wireType == wireVarint {
			_, p2, err := readVarint(data, pos)
			if err != nil {
				return h, err
			}
			pos = p2
		}
	}
	return h, nil
}

// ParseFramePayload extracts JSON payload from a frame.
func ParseFramePayload(f *Frame) (map[string]any, error) {
	if len(f.Payload) == 0 {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(f.Payload, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// CreateFrame builds a frame with JSON-encoded payload.
func CreateFrame(seqID, method int, headers []Header, payload any) *Frame {
	var payloadBytes []byte
	if payload != nil {
		payloadBytes, _ = json.Marshal(payload)
	}
	logID := fmt.Sprintf("%d", seqID)
	return &Frame{
		SeqID:   seqID,
		LogID:   logID,
		Method:  method,
		Headers: headers,
		Payload: payloadBytes,
	}
}
