package conf

type cdcMonitorEntity struct {
	Id       int
	BodyType string
	Body     string
}

type Conf struct {
	Identity     int             `json:"identity_id"`
	Listen       *ListenModel    `json:"listen"`
	SlotConf     *SlotConf       `json:"slot"`
	RabbitMqConf *RabbitConf     `json:"rabbit"`
	Monitors     *[]MonitorModel `json:"monitors"`
}

type ListenModel struct {
	DBType           string `json:"database_type"`
	ConnectionString string `json:"conn"`
}

type SlotConf struct {
	SlotName        string   `json:"slotName"`
	Temporary       bool     `json:"temporary"`
	PluginName      string   `json:"plugin"`
	PluginArguments []string `json:"plugin_args"`
}

type MonitorModel struct {
	Table    string   `json:"table"`
	Schema   string   `json:"schema"`
	Fields   []string `json:"fields"`
	Behavior string   `json:"behavior"`
	// TODO 此版本废弃
	ActionKey   string `json:"action_key"`
	Description string `json:"description"`
}

type RabbitConf struct {
	Conn  string `json:"conn"`
	Queue string `json:"queue"`
}
