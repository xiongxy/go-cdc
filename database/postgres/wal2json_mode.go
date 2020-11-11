package postgres

import (
	"cdc-distribute/model"
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

func (m PgMonitor) Wal2JsonProcess(jsonData string) mapset.Set {
	payload := Wal2JsonParse(jsonData)
	set := mapset.NewSet()
	for _, v := range payload.Change {
		tableKey := v.Schema + "." + v.Table
		_, te := m.TableExistMap[tableKey]
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
					set = set.Union(value)
				}
			}
		}
	}
	return set
}

func (m PgMonitor) Wal2JsonProcess1(jsonData string) mapset.Set {
	payload := Wal2JsonParse(jsonData)
	set := mapset.NewSet()
	for _, v := range payload.Change {
		tableKey := v.Schema + "." + v.Table
		_, te := m.TableExistMap[tableKey]
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
				_, ok := m.QuickReferenceTable[k]
				if ok {
					kind := ""
					switch v.Kind {
					case "insert":
						kind = "I"
						break
					case "update":
						kind = "U"
						break
					case "delete":
						kind = "D"
						break
					}
					data, _ := json.Marshal(m.getRowInfo(v, kind))
					set.Add(model.NewMessageWrapper(model.RabbitMQ, data))
					break
				}
			}
		}
	}
	return set
}

func (m PgMonitor) getRowInfo(change ChangeDef, kind string) model.CdcRowInfo {
	var primaryKey = m.TableColumn[change.Schema+"."+change.Table][0]
	var index = indexOf(primaryKey, change.ColumnNames)

	var msg2send model.CdcRowInfo = model.CdcRowInfo{
		//PatientId: change.ColumnValues[indexPatCol],
		Kind:   kind,
		Table:  change.Table,
		Schema: change.Schema,
		DbName: "CDSS",
	}

	if index != -1 {
		msg2send.KeyValue = change.ColumnValues[index]
		msg2send.KeyName = primaryKey
	}

	return msg2send
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

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}
