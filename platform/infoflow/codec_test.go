package infoflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	cases := []int{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<21 - 1, 1 << 21, 1<<28 - 1}
	for _, v := range cases {
		encoded := writeVarint(v)
		decoded, pos, err := readVarint(encoded, 0)
		if err != nil {
			t.Fatalf("readVarint(%d): %v", v, err)
		}
		if pos != len(encoded) {
			t.Fatalf("varint %d: consumed %d bytes, encoded %d bytes", v, pos, len(encoded))
		}
		if decoded != v {
			t.Fatalf("varint round-trip failed: wrote %d, read %d", v, decoded)
		}
	}
}

func TestVarintEdgeCases(t *testing.T) {
	// Zero should encode to single byte
	enc := writeVarint(0)
	if len(enc) != 1 || enc[0] != 0 {
		t.Fatalf("varint(0) = %v, want [0]", enc)
	}

	// Truncated varint should error
	_, _, err := readVarint([]byte{0x80}, 0)
	if err == nil {
		t.Fatal("expected error on truncated varint")
	}

	// Overflow protection
	overflow := bytes.Repeat([]byte{0x80}, 10)
	_, _, err = readVarint(overflow, 0)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		frame Frame
	}{
		{
			name: "minimal",
			frame: Frame{
				SeqID:  1,
				Method: FrameMethodControl,
			},
		},
		{
			name: "heartbeat ping",
			frame: Frame{
				SeqID:   42,
				LogID:   "42",
				Method:  FrameMethodControl,
				Headers: []Header{{Key: "type", Value: "ping"}},
			},
		},
		{
			name: "data with payload",
			frame: Frame{
				SeqID:   100,
				LogID:   "log-100",
				Service: 7,
				Method:  FrameMethodData,
				Headers: []Header{
					{Key: "type", Value: "event"},
					{Key: "source", Value: "im"},
				},
				Payload: []byte(`{"eventtype":"MESSAGE_RECEIVE","groupid":12345}`),
			},
		},
		{
			name: "response frame",
			frame: Frame{
				SeqID:  999,
				LogID:  "resp-999",
				Method: FrameMethodResponse,
				Headers: []Header{
					{Key: "status", Value: "ok"},
				},
				Payload: []byte(`{"code":0,"message":"success"}`),
			},
		},
		{
			name: "large payload",
			frame: Frame{
				SeqID:   2000,
				LogID:   "big",
				Method:  FrameMethodData,
				Payload: bytes.Repeat([]byte("x"), 10000),
			},
		},
		{
			name: "unicode headers",
			frame: Frame{
				SeqID:  3,
				Method: FrameMethodData,
				Headers: []Header{
					{Key: "用户", Value: "张三"},
					{Key: "emoji", Value: "🎉🚀"},
				},
				Payload: []byte(`{"content":"你好世界"}`),
			},
		},
		{
			name: "many headers",
			frame: Frame{
				SeqID:  50,
				Method: FrameMethodRequest,
				Headers: func() []Header {
					h := make([]Header, 20)
					for i := range h {
						h[i] = Header{Key: fmt.Sprintf("key-%d", i), Value: fmt.Sprintf("val-%d", i)}
					}
					return h
				}(),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeFrame(&tc.frame)
			decoded, err := DecodeFrame(encoded)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}

			if decoded.SeqID != tc.frame.SeqID {
				t.Errorf("SeqID: got %d, want %d", decoded.SeqID, tc.frame.SeqID)
			}
			if decoded.LogID != tc.frame.LogID {
				t.Errorf("LogID: got %q, want %q", decoded.LogID, tc.frame.LogID)
			}
			if decoded.Service != tc.frame.Service {
				t.Errorf("Service: got %d, want %d", decoded.Service, tc.frame.Service)
			}
			if decoded.Method != tc.frame.Method {
				t.Errorf("Method: got %d, want %d", decoded.Method, tc.frame.Method)
			}
			if len(decoded.Headers) != len(tc.frame.Headers) {
				t.Fatalf("Headers count: got %d, want %d", len(decoded.Headers), len(tc.frame.Headers))
			}
			for i, h := range decoded.Headers {
				if h.Key != tc.frame.Headers[i].Key || h.Value != tc.frame.Headers[i].Value {
					t.Errorf("Header[%d]: got {%q,%q}, want {%q,%q}",
						i, h.Key, h.Value, tc.frame.Headers[i].Key, tc.frame.Headers[i].Value)
				}
			}
			if !bytes.Equal(decoded.Payload, tc.frame.Payload) {
				t.Errorf("Payload mismatch: got %d bytes, want %d bytes", len(decoded.Payload), len(tc.frame.Payload))
			}
		})
	}
}

func TestCreateFrameAndParse(t *testing.T) {
	payload := map[string]any{
		"ackSeqId": 42,
		"code":     0,
		"message":  "ok",
	}
	frame := CreateFrame(10, FrameMethodData, []Header{{Key: "type", Value: "event_ack"}}, payload)

	if frame.SeqID != 10 {
		t.Fatalf("SeqID: got %d, want 10", frame.SeqID)
	}
	if frame.Method != FrameMethodData {
		t.Fatalf("Method: got %d, want %d", frame.Method, FrameMethodData)
	}

	// Encode → Decode round trip
	encoded := EncodeFrame(frame)
	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}

	// Parse payload back
	parsed, err := ParseFramePayload(decoded)
	if err != nil {
		t.Fatalf("ParseFramePayload: %v", err)
	}
	if parsed["message"] != "ok" {
		t.Fatalf("payload.message: got %v, want 'ok'", parsed["message"])
	}
	if int(parsed["ackSeqId"].(float64)) != 42 {
		t.Fatalf("payload.ackSeqId: got %v, want 42", parsed["ackSeqId"])
	}
}

func TestParseFramePayloadEmpty(t *testing.T) {
	f := &Frame{}
	result, err := ParseFramePayload(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for empty payload, got %v", result)
	}
}

func TestParseFramePayloadInvalidJSON(t *testing.T) {
	f := &Frame{Payload: []byte("not json")}
	_, err := ParseFramePayload(f)
	if err == nil {
		t.Fatal("expected error for invalid JSON payload")
	}
}

func TestDecodeFrameInvalidData(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"truncated tag", []byte{0x80}},
		{"unsupported wire type", []byte{0x0d}}, // field 1, wire type 5 (32-bit)
		{"truncated length", []byte{0x12, 0x80}},
		{"length exceeds buffer", []byte{0x12, 0xff, 0x01}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DecodeFrame(tc.data)
			if tc.name == "empty" {
				// Empty input should produce an empty frame (no error)
				if err != nil {
					t.Fatalf("unexpected error for empty input: %v", err)
				}
				if result.SeqID != 0 {
					t.Fatalf("expected zero frame for empty input")
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error, got frame: %+v", result)
			}
		})
	}
}

func TestEncodeStringFieldEmpty(t *testing.T) {
	result := encodeStringField(1, "")
	if result != nil {
		t.Fatalf("encodeStringField with empty string should return nil, got %v", result)
	}
}

// ─── Fuzz Tests ──────────────────────────────────────────────────────────────

func FuzzFrameRoundTrip(f *testing.F) {
	// Seed corpus
	f.Add(1, "log1", 0, 0, "type", "ping", []byte("{}"))
	f.Add(999, "big-log", 7, 1, "source", "im", []byte(`{"key":"value"}`))
	f.Add(0, "", 0, 3, "", "", []byte{})

	f.Fuzz(func(t *testing.T, seqID int, logID string, service int, method int, hKey string, hVal string, payload []byte) {
		if seqID < 0 || service < 0 || method < 0 || method > 3 {
			t.Skip()
		}
		if len(logID) > 100 || len(hKey) > 100 || len(hVal) > 100 || len(payload) > 10000 {
			t.Skip()
		}

		original := &Frame{
			SeqID:   seqID,
			LogID:   logID,
			Service: service,
			Method:  method,
			Payload: payload,
		}
		if hKey != "" {
			original.Headers = []Header{{Key: hKey, Value: hVal}}
		}

		encoded := EncodeFrame(original)
		decoded, err := DecodeFrame(encoded)
		if err != nil {
			t.Fatalf("DecodeFrame failed: %v", err)
		}

		if decoded.SeqID != original.SeqID {
			t.Errorf("SeqID mismatch: %d vs %d", decoded.SeqID, original.SeqID)
		}
		if decoded.Method != original.Method {
			t.Errorf("Method mismatch: %d vs %d", decoded.Method, original.Method)
		}
		if decoded.Service != original.Service {
			t.Errorf("Service mismatch: %d vs %d", decoded.Service, original.Service)
		}
		if !bytes.Equal(decoded.Payload, original.Payload) {
			t.Error("Payload mismatch")
		}
	})
}

func FuzzDecodeFrame(f *testing.F) {
	// Seed with valid encoded frames
	ping := CreateFrame(1, FrameMethodControl, []Header{{Key: "type", Value: "ping"}}, nil)
	f.Add(EncodeFrame(ping))

	data := CreateFrame(42, FrameMethodData, nil, map[string]any{"hello": "world"})
	f.Add(EncodeFrame(data))

	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic regardless of input
		_, _ = DecodeFrame(data)
	})
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

func BenchmarkEncodeFrame_Small(b *testing.B) {
	f := &Frame{
		SeqID:   42,
		LogID:   "42",
		Method:  FrameMethodControl,
		Headers: []Header{{Key: "type", Value: "ping"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeFrame(f)
	}
}

func BenchmarkEncodeFrame_WithPayload(b *testing.B) {
	payload, _ := json.Marshal(map[string]any{
		"eventtype": "MESSAGE_RECEIVE",
		"groupid":   12345,
		"message": map[string]any{
			"header": map[string]any{"fromuserid": "user1", "messageid": "msg-001"},
			"body":   []map[string]any{{"type": "TEXT", "content": "Hello, world!"}},
		},
	})
	f := &Frame{
		SeqID:   100,
		LogID:   "log-100",
		Service: 7,
		Method:  FrameMethodData,
		Headers: []Header{{Key: "type", Value: "event"}, {Key: "source", Value: "im"}},
		Payload: payload,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeFrame(f)
	}
}

func BenchmarkDecodeFrame_Small(b *testing.B) {
	f := &Frame{
		SeqID:   42,
		LogID:   "42",
		Method:  FrameMethodControl,
		Headers: []Header{{Key: "type", Value: "ping"}},
	}
	data := EncodeFrame(f)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeFrame(data)
	}
}

func BenchmarkDecodeFrame_WithPayload(b *testing.B) {
	payload, _ := json.Marshal(map[string]any{
		"eventtype": "MESSAGE_RECEIVE",
		"groupid":   12345,
		"message": map[string]any{
			"header": map[string]any{"fromuserid": "user1", "messageid": "msg-001"},
			"body":   []map[string]any{{"type": "TEXT", "content": "Hello, world!"}},
		},
	})
	f := &Frame{
		SeqID:   100,
		LogID:   "log-100",
		Service: 7,
		Method:  FrameMethodData,
		Headers: []Header{{Key: "type", Value: "event"}, {Key: "source", Value: "im"}},
		Payload: payload,
	}
	data := EncodeFrame(f)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeFrame(data)
	}
}

func BenchmarkDecodeFrame_LargePayload(b *testing.B) {
	payload := make([]byte, 64*1024)
	rand.Read(payload)
	// Make it valid-ish JSON
	payload = []byte(`{"data":"` + string(bytes.Repeat([]byte("x"), 60000)) + `"}`)
	f := &Frame{
		SeqID:   500,
		Method:  FrameMethodData,
		Payload: payload,
	}
	data := EncodeFrame(f)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeFrame(data)
	}
}

func BenchmarkCreateFrame(b *testing.B) {
	headers := []Header{{Key: "type", Value: "event_ack"}}
	payload := map[string]any{"ackSeqId": 42, "code": 0, "message": "ok"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CreateFrame(i, FrameMethodData, headers, payload)
	}
}

func BenchmarkVarintEncode(b *testing.B) {
	values := []int{0, 1, 127, 128, 16383, 16384, 1 << 21}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeVarint(values[i%len(values)])
	}
}

func BenchmarkVarintDecode(b *testing.B) {
	encoded := writeVarint(1 << 21)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readVarint(encoded, 0)
	}
}
