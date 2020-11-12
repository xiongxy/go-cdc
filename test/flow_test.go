package test

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/core/runner"
	"cdc-distribute/database/postgres"
	"fmt"
	"testing"
	"time"
)

func getConf() conf.Conf {

	// "plugin": "test_decoding"
	// "plugin": "wal2json"
	confStr := `
{
  "identity_id": 1,
  "listen": {
    "database_type": "postgres",
    "conn": "postgres://postgres:postgres@192.168.142.128/postgres?replication=database"
  },
  "slot": {
    "slotName": "test_demo1",
    "temporary": true,
    "plugin": "wal2json", 
    "plugin_args": []
  },
  "monitors": [
    {
      "table": "test",
      "schema": "test",
      "fields": ["name"],
      "behavior": "",
      "action_key": "test1",
      "description": ""
    }
  ]
}
`
	res := conf.Parse(confStr)
	return res
}

func Test_Listen(t *testing.T) {
	res := getConf()
	go process.LoopProcess()
	runner, _ := runner.New(res).Builder()
	go runner.Run()
	select {}
}

func Test_wal2json_Process_Time(t *testing.T) {

	jsonStr := `
{
  "change": [
    {
      "kind": "update",
      "schema": "public",
      "table": "employees",
      "columnnames": ["id", "name", "age"],
      "columntypes": ["integer", "text", "integer"],
      "columnvalues": [4, "奥21", 11],
      "oldkeys": {
        "keynames": ["id", "name", "age"],
        "keytypes": ["integer", "text", "integer"],
        "keyvalues": [4, "奥术大师多11", 12111]
      }
    }
  ]
}
`
	bT := time.Now()
	for i := 0; i < 10000; i++ {
		postgres.Wal2JsonParse(jsonStr)
	}
	eT := time.Since(bT)
	fmt.Println("Run time: ", eT)
}
