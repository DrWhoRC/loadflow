package message

import (
	"bytes"
	"encoding/json"
)

// Codec 负责在 raw bytes 和 (key,payload) 之间转换
type Codec interface {
	Decode(raw []byte) (key []byte, payload []byte, ok bool)
	Encode(key []byte, payload []byte) ([]byte, error)
}

// JSONCodec 是默认实现：Envelope{key,payload}
type JSONCodec struct {
	// KeyField 预留：如果你未来想把字段名从 "key" 改掉，可以扩展
}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Decode：
// - 若 raw 符合 Envelope，则返回 key/payload，ok=true
// - 否则 ok=false（让上层决定降级策略）
func (c *JSONCodec) Decode(raw []byte) (key []byte, payload []byte, ok bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, nil, false
	}
	if raw[0] != '{' {
		return nil, nil, false
	}

	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, nil, false
	}

	if env.Payload != nil {
		payload = []byte(env.Payload)
	}
	if env.Key != nil && len(*env.Key) > 0 {
		key = []byte(*env.Key)
	}
	return key, payload, true
}

func (c *JSONCodec) Encode(key []byte, payload []byte) ([]byte, error) {
	var k *string
	if len(key) > 0 {
		s := string(key)
		k = &s
	}
	env := Envelope{
		Key:     k,
		Payload: payload,
	}
	return json.Marshal(env)
}
