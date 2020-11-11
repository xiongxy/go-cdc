package model

type CdcRowInfo struct {
	KeyName  string
	KeyValue interface{}
	Kind     string //I U D
	Table    string
	Schema   string
	Note     string
	DbName   string
}
