package postgres

import (
	"cdc-distribute/model"
	"encoding/json"
	"github.com/deckarep/golang-set"
	"strings"
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
		_, te := m.rule.TableExistMap[tableKey]
		if !te {
			continue
		} else {
			switch v.Kind {
			case "insert":
				{
					data := Wal2JsonBuildWalData(v)
					set.Add(model.NewMessageWrapper(&data))
					break
				}
			case "update":
				{
					isChange := Wal2JsonHit(m.rule.TableColumn[tableKey], v.ColumnNames, v.ColumnValues, v.OldKeys.KeyNames, v.OldKeys.KeyValues)
					if isChange {
						data := Wal2JsonBuildWalData(v)
						set.Add(model.NewMessageWrapper(&data))
					}
					break
				}
			case "delete":
				{
					data := Wal2JsonBuildWalData(v)
					set.Add(model.NewMessageWrapper(&data))
					break
				}
			}
		}
	}
	return set
}

func Wal2JsonBuildWalData(change ChangeDef) *model.WalData {
	ret := &model.WalData{}
	ret.OperationType = strings.ToUpper(change.Kind)
	ret.Schema = change.Schema
	ret.Table = change.Table

	if len(change.ColumnNames) > 0 {
		ret.NewData = make(map[string]interface{}, len(change.ColumnNames))
	}

	for index, value := range change.ColumnNames {
		ret.NewData[value] = change.ColumnValues[index]
	}

	if len(change.OldKeys.KeyNames) > 0 {
		ret.OldData = make(map[string]interface{}, len(change.OldKeys.KeyNames))
	}

	for index, value := range change.OldKeys.KeyNames {
		ret.OldData[value] = change.OldKeys.KeyValues[index]
	}

	return ret
}

func Wal2JsonHit(fields []string, newColumns []string, newValues []interface{}, oldColumns []string, oldValues []interface{}) bool {
	for _, field := range fields {
		nIndex := indexOf(field, newColumns)
		oIndex := indexOf(field, oldColumns)

		var nValue interface{}
		var oValue interface{}

		if nIndex == -1 {
			nValue = nil
		} else {
			nValue = newValues[nIndex]
		}

		if oIndex == -1 {
			oValue = nil
		} else {
			oValue = oldValues[oIndex]
		}

		if nValue != oValue {
			return true
		}
	}
	return false
}

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}
