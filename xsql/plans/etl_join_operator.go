package plans

import (
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"strings"
	"sync"
)

type EtlJoinPlan struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool
	MaxCountPossible int
	MsgIdCountMap map[interface{}]map[interface{}]interface{}
	mutex sync.RWMutex
	metaArray [6]string
}



/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *EtlJoinPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	//preTime := time.Now().UnixNano()
	//var postListTime , preListTime, preCountTime, postCountTime, preTupleTime, postTupleTime   int64
	//var t1, t2, t3, t4 ,t5, t6, t7, t8 ,t9 int64
	//log := ctx.GetLogger()
	//log.Debugf("project plan receive %s", data)
	//fmt.Println("pp point   ",pp)
	//pp.mutex.Lock()
	//defer pp.mutex.Unlock()
	//fmt.Println("EtlJoinPlan apply pre data is ", data)
	//var results []map[string]interface{}
	//t1 = time.Now().UnixNano()


	//switch input := data.(type) {
	//case error:
	//	return input
	//case *xsql.Tuple:

		//preTupleTime = time.Now().UnixNano()
		message := data.(*xsql.Tuple).Message
		//t2 = time.Now().UnixNano()
		//pp.mutex.RLock()
		//defer pp.mutex.RUnlock()
		msg,ok := pp.MsgIdCountMap[message["msgId"]]
		//t9 = time.Now().UnixNano()
		//fmt.Println("etl join msg map out ", msg, " message ", message, "ok ", ok)
		if ok {
			msg[message["obsType"]] = message["obsVal"]

			//fmt.Println("etl Join msg map ", msg)
			if len(msg) == pp.MaxCountPossible {
				//t3 = time.Now().UnixNano()
				tuple := new(xsql.Tuple)
				m := make(map[string]interface{})
				m["msgId"] = message["msgId"]
				m["meta"] = message["meta"]
				m["obsType"] = "JoinedValue"

				//preListTime = time.Now().UnixNano()

				var builer strings.Builder
				for _,str := range pp.metaArray{
					//fmt.Println("------------", str, msg[str])
					builer.WriteString(msg[str].(string) + ",")
				}
				//buffer.

				m["obsVal"] = builer.String()


				//postListTime = time.Now().UnixNano()
				//fmt.Println("EtlJoinPlan list execute time " , postListTime-preListTime)

				 	//msg["source"].(string) + "," + message["meta"].(string) + "," + msg["temperature"].(string) + "," + msg["humidity"].(string)+ "," + msg["light"].(string) +
					//"," + msg["dust"].(string) + "," + msg["airquality_raw"].(string)
				//metar := make(map[string]interface{})
				//tuple := new(xsql.Tuple)
				tuple.Message = m

				//result := make(map[string]interface{})
				//result["demo"] = tuple
				//results = append(results, result)
				//fmt.Println("Etl Join OP OutputController ", tuple)
				Oc.Data <- tuple
				delete(pp.MsgIdCountMap, message["msgId"])
				//t4 = time.Now().UnixNano()
			} else {
				//t5 = time.Now().UnixNano()
				pp.MsgIdCountMap[message["msgId"]] = msg
				//t6 = time.Now().UnixNano()
			}
		} else {
			//t7 = time.Now().UnixNano()
			msgm := make(map[interface{}]interface{})
			msgm[message["obsType"]] = message["obsVal"]
			pp.MsgIdCountMap[message["msgId"]] = msgm
			//t8 = time.Now().UnixNano()

			//fmt.Println("etl join msgm ", msgm)
		}




	//default:
	//	return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	//}
	//postTime := time.Now().UnixNano()

	//fmt.Println("EtlJoinPlan execute time " , t2-t1, t9-t2, t4-t3, t6-t5, t8-t7 , postTime-preTime, t1-preTime)
	//fmt.Println("EtlJoinPlan execute time " , postTime-preTime)

	//fmt.Println("EtlJoinPlan apply post data is ", tuple, len(tuple.Message))
	//if len(tuple.Message) == 0 {
	//	return nil
	//} else {
	//	return tuple
	//}
	//return tuple
	//if ret, err := json.Marshal(results); err == nil {
	//	return ret
	//} else {
	//	return fmt.Errorf("run Select error: %v", err)
	//}
	return  nil
}

func (pp *EtlJoinPlan ) Prepare(){
	//pp.metaArray = list.New()
	pp.metaArray[0] = "source"
	pp.metaArray[1] = "temperature"
	pp.metaArray[2] = "humidity"
	pp.metaArray[3] = "light"
	pp.metaArray[4] = "dust"
	pp.metaArray[5] = "airquality_raw"
}

