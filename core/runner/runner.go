package runner

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/database"
	"fmt"
	"github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"
)

func New(c conf.Conf) *Runner {
	ret := &Runner{
		ConfigModel:         c,
		TableColumn:         make(map[string][]string),
		TableExistMap:       make(map[string]bool),
		QuickReferenceTable: make(map[string]mapset.Set),
	}
	ret.Process = process.NewProcess(c)
	return ret
}

type Runner struct {
	ConfigModel         conf.Conf
	Process             process.Process
	TableColumn         map[string][]string
	TableExistMap       map[string]bool
	QuickReferenceTable map[string]mapset.Set
}

func (s *Runner) Builder() *Runner {
	return s.lexicalization()
}

func (s *Runner) Run() error {
	m, err := database.Selector(s.ConfigModel.Listen.DBType, s.ConfigModel, s.TableColumn, s.TableExistMap, s.QuickReferenceTable)
	if err != nil {
		logrus.Errorf("create replication slot err: %v", err)
		return fmt.Errorf("failed to create replication slot: %s", err)
	}
	return m.Run(process.Put)
}

func (s *Runner) lexicalization() *Runner {

	for _, v := range *s.ConfigModel.Monitors {
		k := v.Schema + "." + v.Table
		_, ok := s.TableExistMap[k]
		if !ok {
			s.TableExistMap[k] = true
		}
	}

	for _, v := range *s.ConfigModel.Monitors {
		k := v.Schema + "." + v.Table
		_, ok := s.TableColumn[k]
		if !ok {
			s.TableColumn[k] = v.Fields
		}
	}

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
				capital, ok := s.QuickReferenceTable[k]
				if ok {
					capital.Add(v.ActionKey)
				} else {
					set := mapset.NewSet()
					set.Add(v.ActionKey)
					s.QuickReferenceTable[k] = set
				}
			}
		}
	}
	return s
}
