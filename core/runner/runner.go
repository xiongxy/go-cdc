package runner

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/database"
	"cdc-distribute/model"
	"fmt"
	"github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"
	"time"
)

func New(c conf.Conf) *Runner {
	ret := &Runner{
		ConfigModel: c,
		dataChannel: make(chan []*model.MessageWrapper, 20480),
		QuickCheckRule: struct {
			TableColumn         map[string][]string
			TableExistMap       map[string]bool
			QuickReferenceTable map[string]mapset.Set
		}{TableColumn: make(map[string][]string), TableExistMap: make(map[string]bool), QuickReferenceTable: make(map[string]mapset.Set)},
	}
	return ret
}

type Runner struct {
	ConfigModel conf.Conf
	Process     process.Process
	Monitor     database.Monitor

	dataChannel chan []*model.MessageWrapper
	dataList    []*model.MessageWrapper

	QuickCheckRule model.QuickCheckRule
}

func (s *Runner) Builder() (*Runner, error) {
	s.lexicalization()

	s.Process = process.NewProcess(s.ConfigModel)
	go s.runProcessLoop()

	m, err := database.Selector(s.ConfigModel.Listen.DBType, s.ConfigModel, s.QuickCheckRule)
	if err != nil {
		logrus.Errorf("create replication slot err: %v", err)
		return nil, fmt.Errorf("failed to create replication slot: %s", err)
	}

	s.Monitor = m

	return s, nil
}

func (s *Runner) Run() error {
	return s.Monitor.Run(s.dataChannel)
}

func (s *Runner) lexicalization() {
	quickRule := s.QuickCheckRule
	// init TableExistMap
	for _, v := range *s.ConfigModel.Monitors {
		k := v.Schema + "." + v.Table
		_, ok := quickRule.TableExistMap[k]
		if !ok {
			quickRule.TableExistMap[k] = true
		}
	}

	//init TableColumn
	for _, v := range *s.ConfigModel.Monitors {
		k := v.Schema + "." + v.Table
		_, ok := quickRule.TableColumn[k]
		if !ok {
			quickRule.TableColumn[k] = v.Fields
		}
	}

	//init QuickReferenceTable
	for _, v := range *s.ConfigModel.Monitors {
		var behaviors []string
		if v.Behavior == "" {
			behaviors = append(behaviors, "update")
			behaviors = append(behaviors, "insert")
			behaviors = append(behaviors, "delete")
		} else {
			behaviors = append(behaviors, v.Behavior)
		}
		for _, behavior := range behaviors {
			for _, field := range v.Fields {
				k := behavior + ":" + v.Schema + "." + v.Table + "." + field
				capital, ok := quickRule.QuickReferenceTable[k]
				if ok {
					capital.Add(v.ActionKey)
				} else {
					set := mapset.NewSet()
					set.Add(v.ActionKey)
					quickRule.QuickReferenceTable[k] = set
				}
			}
		}
	}
}

func (s *Runner) runProcessLoop() {
	timer := time.NewTimer(time.Second)
	for {
		var needFlush bool

		select {
		case <-timer.C:
			needFlush = true
		case dataList := <-s.dataChannel:
			for _, data := range dataList {
				s.dataList = append(s.dataList, data)
			}
			needFlush = len(s.dataList) >= 20000
		}

		if needFlush {
			s.flush()
			resetTimer(timer, time.Second)
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	// reset timer
	select {
	case <-t.C:
	default:
	}
	t.Reset(d)
}

func (s *Runner) flush() error {
	defer func() {
		if len(s.dataList) > 0 {
			//if err != nil {
			//	monitor.IncreaseErrorCount(h.sub.SlotName, 1)
			//} else {
			//	monitor.IncreaseSuccessCount(h.sub.SlotName, len(h.datas))
			//}
		}
		//h.callback(h.maxPos)
		s.dataList = nil
	}()

	if len(s.dataList) == 0 {
		return nil
	}

	return s.Process.Write(s.dataList...)
}
