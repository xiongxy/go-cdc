package postgres

import (
	"cdc-distribute/conf"
	"context"
	"github.com/deckarep/golang-set"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"log"
	"strconv"
	"time"
)

func NewPostgresMonitor(identity int, listen conf.ListenModel, tableMap map[string]bool, quickTable map[string]mapset.Set) *Monitor {
	return &Monitor{
		Identity:            identity,
		Listen:              listen,
		TableMap:            tableMap,
		QuickReferenceTable: quickTable,
	}
}

type Monitor struct {
	Identity            int
	Listen              conf.ListenModel
	TableMap            map[string]bool
	QuickReferenceTable map[string]mapset.Set
}

func (m Monitor) Monitor(exec func(mapset.Set)) {
	connStr := m.Listen.ConnectionString + "?replication=database"

	outputPlugin := m.Listen.PluginName

	pluginArguments := m.Listen.PluginArguments

	slotName := "cdc_" + strconv.Itoa(m.Identity) + "_slot"

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalln("failed to connect to postgres server:", err)
	}
	defer conn.Close(context.Background())

	if outputPlugin == "pgoutput" {
		result := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+slotName)
		_, err := result.ReadAll()
		if err != nil {
			log.Fatalln("drop publication if exists error", err)
		}

		result = conn.Exec(context.Background(), "CREATE PUBLICATION "+slotName+" FOR ALL TABLES;")
		_, err = result.ReadAll()
		if err != nil {
			log.Fatalln("create publication error", err)
		}
		log.Println("create publication pglogrepl_demo")

		pluginArguments = []string{"proto_version '1'", "publication_names '" + slotName + "'"}
	}

	systemInfo, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", systemInfo.SystemID, "Timeline:", systemInfo.Timeline, "XLogPos:", systemInfo.XLogPos, "DBName:", systemInfo.DBName)

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, systemInfo.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := systemInfo.XLogPos
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

				switch m.Listen.PluginName {
				case "wal2json":
					ta := m.Wal2JsonProcess(string(xld.WALData))
					exec(ta)
					break
				case "pgoutput":
					m.PgOutputProcess(string(xld.WALData))
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}
