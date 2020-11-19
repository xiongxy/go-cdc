package postgres

import (
	"cdc-distribute/conf"
	"cdc-distribute/log"
	"cdc-distribute/model"
	"context"
	mapSet "github.com/deckarep/golang-set"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/satori/go.uuid"
	"time"
)

func NewPostgresMonitor(conf conf.Conf, rule model.QuickCheckRule) *PgMonitor {
	return &PgMonitor{
		conf: conf,
		rule: rule,
	}
}

type PgMonitor struct {
	conf conf.Conf
	rule model.QuickCheckRule
}

func (m PgMonitor) Run(dataChan chan []*model.MessageWrapper) error {

	slotConf := m.conf.Listen.SlotConf

	outputPlugin := slotConf.PluginName

	pluginArguments := slotConf.PluginArguments

	var slotName = ""
	if slotConf.SlotName == "" {
		slotName = "slot_" + uuid.NewV4().String()
	} else {
		slotName = slotConf.SlotName
	}

	connStr := m.conf.Listen.ConnectionString

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		log.Logger.Fatalln("failed to connect to postgres server:", err)
	}
	defer conn.Close(context.Background())

	// get IdentifySystem
	systemInfo, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Logger.Fatalln("IdentifySystem failed:", err)
	}
	log.Logger.Println("SystemID:", systemInfo.SystemID, "Timeline:", systemInfo.Timeline, "XLogPos:", systemInfo.XLogPos, "DBName:", systemInfo.DBName)

	// is temporary slot
	temporary := slotConf.Temporary
	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	if err != nil {
		log.Logger.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Logger.Println("Created temporary replication slot:", slotName)

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, systemInfo.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Logger.Fatalln("StartReplication failed:", err)
	}
	log.Logger.Println("Logical replication started on slot", slotName)

	clientXLogPos := systemInfo.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Logger.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Logger.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Logger.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Logger.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Logger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Logger.Fatalln("ParseXLogData failed:", err)
				}
				m.modeProcess(dataChan, xld)
				log.Logger.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Logger.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}

func (m PgMonitor) modeProcess(dataChan chan []*model.MessageWrapper, xld pglogrepl.XLogData) {
	set := mapSet.NewSet()

	switch m.conf.Listen.SlotConf.PluginName {
	case "wal2json":
		set = m.Wal2JsonProcess(string(xld.WALData))
		break
	case "test_decoding":
		set = m.TestDecodingProcess(xld)
		break
	}

	slice := set.ToSlice()
	dataList := make([]*model.MessageWrapper, 0)
	for _, v := range slice {
		message := v.(*model.MessageWrapper)
		dataList = append(dataList, message)
	}
	dataChan <- dataList
}
