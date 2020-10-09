package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"reflect"
)

type EtlJoinPlan struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool
	MaxCountPossible int
	MsgIdCountMap map[string]map[string]string 
}



/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *EtlJoinPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	fmt.Println("EtlJoinPlan apply pre data is ", data, reflect.TypeOf(data))
	var results []map[string]interface{}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		message := input.Message
		msg,ok := pp.MsgIdCountMap[message["msgId"].(string)]
		fmt.Println("etl join msg map out ", msg, " message ", message, "ok ", ok)
		if ok {
			msg[message["obsType"].(string)] = message["obsVal"].(string)
			fmt.Println("etl Join msg map ", msg)
			if len(msg) == pp.MaxCountPossible {
				m := make(map[string]interface{})
				m["msgId"] = message["msgId"].(string)
				m["meta"] = message["meta"].(string)
				m["obsType"] = "JoinedValue"
				m["obsVal"] =  msg["source"] + "," + message["meta"].(string) + "," + msg["temperature"] + "," + msg["humidity"] + "," + msg["light"] +
					"," + msg["dust"] + "," + msg["airquality_raw"]
				//metar := make(map[string]interface{})
				tuple := new(xsql.Tuple)
				tuple.Message = m

				fmt.Println("Etl Join OP OutputController ", tuple)
				Oc.Data <- tuple
				delete(pp.MsgIdCountMap, message["msgId"].(string))
			} else {
				pp.MsgIdCountMap[message["msgId"].(string)] = msg
			}
		} else {
			msgm := make(map[string]string)
			msgm[message["obsType"].(string)] = message["obsVal"].(string)
			pp.MsgIdCountMap[message["msgId"].(string)] = msgm
			fmt.Println("etl join msgm ", msgm)
		}


	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}

	fmt.Println("EtlJoinPlan apply post data is ", results)
	if ret, err := json.Marshal(results); err == nil {
		return ret
	} else {
		return fmt.Errorf("run Select error: %v", err)
	}
}

func (pp *EtlJoinPlan) getVE(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	} else {
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
	}
}

func (pp *EtlJoinPlan ) Prepare(){

}

