package conf

type cdcMonitorEntity struct {
	Id       int
	BodyType string
	Body     string
}

type ConfigModel struct {
	Identity int
	Listen   ListenModel    `json:"listen"`
	Monitors []MonitorModel `json:"monitors"`
}

type ListenModel struct {
	Database         string   `json:"database"`
	ConnectionString string   `json:"connStr"`
	PluginName       string   `json:"plugin"`
	PluginArguments  []string `json:"plugin_args"`
}

type MonitorModel struct {
	Table       string   `json:"table"`
	Schema      string   `json:"schema"`
	Fields      []string `json:"fields"`
	Behavior    string   `json:"behavior"`
	ActionKey   string   `json:"action_key"`
	Description string   `json:"description"`
}
