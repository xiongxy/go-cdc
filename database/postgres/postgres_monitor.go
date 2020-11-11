package postgres

import (
	"cdc-distribute/conf"
	"context"
	"github.com/deckarep/golang-set"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/satori/go.uuid"
	"log"
	"time"
)

func NewPostgresMonitor(conf conf.Conf, tableColumn map[string][]string, tableMap map[string]bool, quickTable map[string]mapset.Set) *PgMonitor {
	return &PgMonitor{
		Identity:            conf.Identity,
		Conf:                conf,
		TableColumn:         tableColumn,
		TableExistMap:       tableMap,
		QuickReferenceTable: quickTable,
	}
}

type PgMonitor struct {
	Identity            int
	Conf                conf.Conf
	TableColumn         map[string][]string
	TableExistMap       map[string]bool
	QuickReferenceTable map[string]mapset.Set
}

func (m PgMonitor) Run(exec func(mapset.Set)) error {

	slotConf := m.Conf.SlotConf

	outputPlugin := slotConf.PluginName

	pluginArguments := slotConf.PluginArguments

	var slotName = ""
	if slotConf.SlotName == "" {
		slotName = "slot_" + uuid.NewV4().String()
	} else {
		slotName = slotConf.SlotName
	}

	connStr := m.Conf.Listen.ConnectionString

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalln("failed to connect to postgres server:", err)
	}
	defer conn.Close(context.Background())

	// get IdentifySystem
	systemInfo, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", systemInfo.SystemID, "Timeline:", systemInfo.Timeline, "XLogPos:", systemInfo.XLogPos, "DBName:", systemInfo.DBName)

	// is temporary slot
	temporary := slotConf.Temporary
	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
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
				m.modeProcess(xld, exec)
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}

func (m PgMonitor) modeProcess(xld pglogrepl.XLogData, exec func(mapset.Set)) {

	switch m.Conf.SlotConf.PluginName {
	case "wal2json":
		set := m.Wal2JsonProcess1(string(xld.WALData))
		exec(set)
		break
	case "test_decoding":
		//
		break
	}
}
