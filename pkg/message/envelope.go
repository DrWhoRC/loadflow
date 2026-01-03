package message

import (
	"encoding/json"
)

// Envelope 默认消息格式：
// - Key：可为 nil（表示无粘性/无有序需求）
// - Payload：真正的业务数据（强烈建议仍是 JSON，但不强制）
type Envelope struct {
	Key     *string         `json:"key"`     // null 表示无 key
	Payload json.RawMessage `json:"payload"` // 原样保留 payload bytes
}
