package postgres

//import (
//	"cdc-distribute/util"
//	mapset "github.com/deckarep/golang-set"
//	"github.com/jackc/pglogrepl"
//	"github.com/nickelser/parselogical"
//	"strconv"
//	"strings"
//)
//
//func (m PgMonitor) TestDecodingProcess(jsonData string) mapset.Set {
//	payload := Wal2JsonParse(jsonData)
//	ta := mapset.NewSet()
//	for _, v := range payload.Change {
//		tableKey := v.Schema + "." + v.Table
//		_, te := m.TableExistMap[tableKey]
//		if !te {
//			continue
//		} else {
//			var changeColumns []string
//			if v.Kind == "insert" {
//				changeColumns = v.ColumnNames
//			} else if v.Kind == "update" {
//				changeColumns = compare(v.OldKeys.KeyValues, v.ColumnValues, v.ColumnNames)
//			} else if v.Kind == "delete" {
//				changeColumns = v.ColumnNames
//			}
//
//			for _, changeColumn := range changeColumns {
//				k := v.Kind + ":" + v.Schema + "." + v.Table + "." + changeColumn
//				value, ok := m.QuickReferenceTable[k]
//				if ok {
//					ta = ta.Union(value)
//				}
//			}
//		}
//	}
//	return ta
//}
//
//func parse(msg *pglogrepl.XLogData) (*WalData, error) {
//	result := parselogical.NewParseResult(util.Bytes2String(msg.WALData))
//	if err := result.Parse(); err != nil {
//		return nil, err
//	}
//	var ret = NewWalData()
//
//	var schema, table string
//	if result.Relation != "" {
//		i := strings.IndexByte(result.Relation, '.')
//		if i < 0 {
//			table = result.Relation
//		} else {
//			schema = result.Relation[:i]
//			table = result.Relation[i+1:]
//		}
//
//		ret.Schema = schema
//		ret.Table = table
//	}
//	ret.Pos = msg.WalStart
//	switch result.Operation {
//	case "INSERT":
//		ret.OperationType = Insert
//	case "UPDATE":
//		ret.OperationType = Update
//	case "DELETE":
//		ret.OperationType = Delete
//	case "BEGIN":
//		ret.OperationType = Begin
//	case "COMMIT":
//		ret.OperationType = Commit
//	}
//
//	if len(result.Columns) > 0 {
//		ret.Data = make(map[string]interface{}, len(result.Columns))
//	}
//	for key, column := range result.Columns {
//		if column.Quoted {
//			ret.Data[key] = column.Value
//			continue
//		}
//
//		if column.Value == "null" {
//			ret.Data[key] = nil
//			continue
//		}
//
//		if val, err := strconv.ParseInt(column.Value, 10, 64); err == nil {
//			ret.Data[key] = val
//			continue
//		}
//		if val, err := strconv.ParseFloat(column.Value, 64); err == nil {
//			ret.Data[key] = val
//			continue
//		}
//		ret.Data[key] = column.Value
//	}
//
//	return ret, nil
//}
