package core

import (
	"cdc-distribute/conf"
	"cdc-distribute/database"
	"github.com/deckarep/golang-set"
)

func NewRunner(c conf.ConfigModel) *Runner {
	return &Runner{
		ConfigModel:         c,
		TableMap:            make(map[string]bool),
		QuickReferenceTable: make(map[string]mapset.Set),
	}
}

type Runner struct {
	ConfigModel         conf.ConfigModel
	TableMap            map[string]bool
	QuickReferenceTable map[string]mapset.Set
}

func (s *Runner) Builder() *Runner {
	return s.lexicalization()
}

func (s *Runner) Run() {
	m, err := database.Selector(s.ConfigModel.Listen.Database, s.ConfigModel.Identity, s.ConfigModel.Listen, s.TableMap, s.QuickReferenceTable)
	if err != nil {

	} else {
		m.Monitor(ProcessAction)
	}
}

func (s *Runner) lexicalization() *Runner {

	for _, v := range s.ConfigModel.Monitors {
		k := v.Schema + "." + v.Table
		_, ok := s.TableMap[k]
		if !ok {
			s.TableMap[k] = true
		}
	}

	for _, v := range s.ConfigModel.Monitors {
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
