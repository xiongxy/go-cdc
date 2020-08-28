package postgres

import (
	"encoding/json"
	"github.com/deckarep/golang-set"
)

type Payload struct {
	Change []ChangeDef `json:"change"`
}

type ChangeDef struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	ColumnValues []interface{} `json:"columnvalues"`
	OldKeys      OldKeysDef    `json:"oldkeys"`
}

type OldKeysDef struct {
	KeyNames  []string      `json:"keynames"`
	KeyTypes  []string      `json:"keytypes"`
	KeyValues []interface{} `json:"keyvalues"`
}

func Wal2JsonParse(jsonData string) Payload {
	var payload Payload
	_ = json.Unmarshal([]byte(jsonData), &payload)
	return payload
}

func (m Monitor) Wal2JsonProcess(jsonData string) mapset.Set {
	payload := Wal2JsonParse(jsonData)
	ta := mapset.NewSet()
	for _, v := range payload.Change {
		tableKey := v.Schema + "." + v.Table
		_, te := m.TableMap[tableKey]
		if !te {
			continue
		} else {
			var changeColumns []string
			if v.Kind == "insert" {
				changeColumns = v.ColumnNames
			} else if v.Kind == "update" {
				changeColumns = compare(v.OldKeys.KeyValues, v.ColumnValues, v.ColumnNames)
			} else if v.Kind == "delete" {
				changeColumns = v.ColumnNames
			}

			for _, changeColumn := range changeColumns {
				k := v.Kind + ":" + v.Schema + "." + v.Table + "." + changeColumn
				value, ok := m.QuickReferenceTable[k]
				if ok {
					ta = ta.Union(value)
				}
			}
		}
	}
	return ta
}

func compare(old []interface{}, current []interface{}, columns []string) []string {
	var changeColumns []string
	len := len(old)
	for i := 0; i < len; i++ {
		ok := old[i] == current[i]
		if !ok {
			changeColumns = append(changeColumns, columns[i])
		}
	}
	return changeColumns
}
