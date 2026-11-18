package hub

import "time"

// Message is a single retained message in a channel's ring buffer.
type Message struct {
	Body      []byte
	Timestamp time.Time
}

// Clone returns a deep copy of the message.
func (m *Message) Clone() *Message {
	return &Message{
		Body:      append([]byte(nil), m.Body...),
		Timestamp: m.Timestamp,
	}
}