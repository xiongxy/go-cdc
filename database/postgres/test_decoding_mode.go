package postgres

import (
	"cdc-distribute/model"
	"cdc-distribute/util"
	mapset "github.com/deckarep/golang-set"
	"github.com/jackc/pglogrepl"
	"github.com/nickelser/parselogical"
	"strconv"
	"strings"
)

func (m PgMonitor) TestDecodingProcess(msg pglogrepl.XLogData) mapset.Set {
	set := mapset.NewSet()
	paresResult := TestDecodingParse(msg)
	hit := TestDecodingHit(paresResult, &m.rule)
	if hit {
		if paresResult != nil {
			walData := TestDecodingBuildWalData(paresResult)
			set.Add(model.NewMessageWrapper(&walData))
		}
	}
	return set
}

func TestDecodingParse(msg pglogrepl.XLogData) *parselogical.ParseResult {
	result := parselogical.NewParseResult(util.Bytes2String(msg.WALData))
	if err := result.Parse(); err != nil {
		return nil
	}
	return result
}

func TestDecodingHit(paresResult *parselogical.ParseResult, rule *model.QuickCheckRule) bool {
	column := rule.TableColumn[paresResult.Relation]

	var nValue interface{}
	var oValue interface{}

	for _, v := range column {

		if newColumn, ok := paresResult.Columns[v]; ok {
			nValue = newColumn.Value
		} else {
			nValue = nil
		}

		if oldColumn, ok := paresResult.OldColumns[v]; ok {
			oValue = oldColumn.Value
		} else {
			oValue = nil
		}

		if nValue != oValue {
			return true
		}
	}
	return false
}

func TestDecodingBuildWalData(paresResult *parselogical.ParseResult) *model.WalData {
	ret := &model.WalData{}

	var schema, table string
	if paresResult.Relation != "" {
		i := strings.IndexByte(paresResult.Relation, '.')
		if i < 0 {
			table = paresResult.Relation
		} else {
			schema = paresResult.Relation[:i]
			table = paresResult.Relation[i+1:]
		}
		ret.Schema = schema
		ret.Table = table
	}

	if len(paresResult.Columns) > 0 {
		ret.NewData = make(map[string]interface{}, len(paresResult.Columns))
	}
	for key, column := range paresResult.Columns {
		if column.Quoted {
			ret.NewData[key] = column.Value
			continue
		}

		if column.Value == "null" {
			ret.NewData[key] = nil
			continue
		}

		if val, err := strconv.ParseInt(column.Value, 10, 64); err == nil {
			ret.NewData[key] = val
			continue
		}
		if val, err := strconv.ParseFloat(column.Value, 64); err == nil {
			ret.NewData[key] = val
			continue
		}
		ret.NewData[key] = column.Value
	}

	if len(paresResult.OldColumns) > 0 {
		ret.OldData = make(map[string]interface{}, len(paresResult.OldColumns))
	}

	for key, column := range paresResult.OldColumns {
		if column.Quoted {
			ret.OldData[key] = column.Value
			continue
		}

		if column.Value == "null" {
			ret.OldData[key] = nil
			continue
		}

		if val, err := strconv.ParseInt(column.Value, 10, 64); err == nil {
			ret.OldData[key] = val
			continue
		}
		if val, err := strconv.ParseFloat(column.Value, 64); err == nil {
			ret.OldData[key] = val
			continue
		}
		ret.OldData[key] = column.Value
	}

	return ret
}
