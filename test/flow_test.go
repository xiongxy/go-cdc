package test

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/core/runner"
	"cdc-distribute/database/postgres"
	"cdc-distribute/log"
	"cdc-distribute/model"
	"fmt"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/sirupsen/logrus"
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
  "conn": "postgres://postgres:postgres@192.168.142.128/postgres?replication=database",
	"slot": {
		"slotName": "test_demo1",
		"temporary": true,
		"plugin": "test_decoding",
		"plugin_args": []
	  }
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
],
"rabbit":{
    "conn" :"amqp://admin:admin@172.16.127.100:26174",
    "queue":"cdc_mq_demo_one"
}
}
`
	res := conf.Parse(confStr)
	return res
}

func Test_Listen(t *testing.T) {
	res := getConf()
	runner, err := runner.New(res).Builder()
	if err != nil {
		log.Logger.WithFields(logrus.Fields{
			"err":    err,
			"config": res,
		}).Error("create runner err")
	} else {
		go runner.Run()
	}
	select {}
}

func Test_RabbitMq(t *testing.T) {
	res := getConf()
	processIn := process.NewProcess(res)

	message := model.MessageWrapper{
		MessageContent: &model.WalData{
			Table:  "xxx",
			Schema: "xxx",
		},
	}
	_ = processIn.Write(&message)
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

func Test_Rabbit(t *testing.T) {
	conn, err := rabbitmq.Dial("amqp://admin:admin@172.16.127.100:25074/cdss_dev")
	if err != nil {
		print(err)
	} else {
		print(conn)
	}
}
