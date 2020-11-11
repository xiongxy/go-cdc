package model

import "sync"

type MessageWrapper struct {
	TransmissionMode TransmissionModeType
	MessageContent   interface{}
}

// Reset for reuse
func (m *MessageWrapper) Reset() {
	m.MessageContent = nil
	m.TransmissionMode = RabbitMQ
}

var messagePool = sync.Pool{New: func() interface{} { return &MessageWrapper{} }}

// NewWalData get data from pool
func NewMessageWrapper(t TransmissionModeType, content interface{}) *MessageWrapper {
	message := newMessageWrapper()
	message.TransmissionMode = t
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

type TransmissionModeType int32

const (
	RabbitMQ TransmissionModeType = 1
)
