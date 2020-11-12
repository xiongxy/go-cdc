package model

import "sync"

type MessageWrapper struct {
	MessageContent interface{}
}

// Reset for reuse
func (m *MessageWrapper) Reset() {
	m.MessageContent = nil
}

var messagePool = sync.Pool{New: func() interface{} { return &MessageWrapper{} }}

// NewWalData get data from pool
func NewMessageWrapper(content interface{}) *MessageWrapper {
	message := newMessageWrapper()
	message.MessageContent = content
	return message
}

func newMessageWrapper() *MessageWrapper {
	data := messagePool.Get().(*MessageWrapper)
	data.Reset()
	return data
}

// PutWalData putback data to pool
func PutWalData(data *MessageWrapper) {
	messagePool.Put(data)
}
