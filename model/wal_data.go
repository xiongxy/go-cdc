package model

type WalData struct {
	OperationType string
	Schema        string
	Table         string
	NewData       map[string]interface{}
	OldData       map[string]interface{}
}
