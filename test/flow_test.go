package test

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/core/runner"
	"cdc-distribute/database/postgres"
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"log"
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
    "temporary": false,
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
	runner.New(res).Builder().Run()
}

func Test_Process_Time(t *testing.T) {

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
	tableMap := make(map[string]bool)
	tableMap["public.employees"] = true

	quickTable := make(map[string]mapset.Set)
	quickTable["update:public.employees.name"] = mapset.NewSetFromSlice([]interface{}{"Biology", "Chemistry"})
	quickTable["update:public.employees.age"] = mapset.NewSetFromSlice([]interface{}{"22", "22"})

	monitor := postgres.NewPostgresMonitor(getConf(), nil, tableMap, quickTable)
	bT := time.Now()
	for i := 0; i < 10000; i++ {
		monitor.Wal2JsonProcess(jsonStr)
	}
	eT := time.Since(bT)
	fmt.Println("Run time: ", eT)
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

func Test_test(t *testing.T) {

	connStr := "postgres://postgres:postgres@192.168.227.129/example_db?replication=database"
	const outputPlugin = "pgoutput"
	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	var pluginArguments []string
	if outputPlugin == "pgoutput" {
		result := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pglogrepl_demo;")
		_, err := result.ReadAll()
		if err != nil {
			log.Fatalln("drop publication if exists error", err)
		}

		result = conn.Exec(context.Background(), "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")
		_, err = result.ReadAll()
		if err != nil {
			log.Fatalln("create publication error", err)
		}
		log.Println("create publication pglogrepl_demo")

		pluginArguments = []string{"proto_version '1'", "publication_names 'pglogrepl_demo'"}
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "pglogrepl_demo"

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}

	}
}
