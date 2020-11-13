package plans

import (
	"bufio"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"os"
	"strings"
)

type AnnotationPlan struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool
	MaxCountPossible int
	MsgIdCountMap map[string]map[string]string
	AnnotationMap map[string]string
}



/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *AnnotationPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	//preTime := time.Now().UnixNano()

	a := 1
	for i:=1; i<6000; i++ {
		a++
	}
	//log := ctx.GetLogger()
	//log.Debugf("project plan receive %s", data)
	//fmt.Println("AnnotationPlan apply pre data is ", data, reflect.TypeOf(data))
	//var results []map[string]interface{}
	tuple := new(xsql.Tuple)
	//switch input := data.(type) {
	//case error:
	//	return input
	//case *xsql.Tuple:
		message := data.(*xsql.Tuple).Message
		source := strings.Split(message["OBSVAL"].(string), ",")[1]


		line := pp.AnnotationMap[source]
		//fmt.Println("-----------", source, line)
		message["OBSVAL"] = message["OBSVAL"].(string) + "," + line
		message["op"] = message["op"].(string) + "-ANNO"

		//message["MSGID"] = message["MSGID"]
		//message["META"] = message["META"]
		//message["TIMESTAMP"] = message["TIMESTAMP"]
		//message["SPOUTTIMESTAMP"] = message["SPOUTTIMESTAMP"]
		//message["CHAINSTAMP"] = message["CHAINSTAMP"]

		if line == "" {
			message["OBSVAL"] = message["OBSVAL"].(string) + "," + "ece,digital"
		}
	//fmt.Println("line --- ", line, message["OBSVAL"])

		//message["obsVal"] = message["obsVal"].(string)
		//tuple := new(xsql.Tuple)
		tuple.Message = message
		//fmt.Println("Annotation tuple data ", tuple)


		//result := make(map[string]interface{})
		//result["demo"] = tuple
		//results = append(results, result)

		Oc.Data <- tuple
		//msg,ok := pp.MsgIdCountMap[message["msgId"].(string)]
		//fmt.Println("etl join msg map out ", msg, " message ", message, "ok ", ok)
		//if ok {
		//	msg[message["obsType"].(string)] = message["obsVal"].(string)
		//	fmt.Println("etl Join msg map ", msg)
		//	if len(msg) == pp.MaxCountPossible {
		//		m := make(map[string]interface{})
		//		m["msgId"] = msg["msgId"]
		//		m["meta"] = msg["meta"]
		//		m["obsType"] = "JoinedValue"
		//		m["obsVal"] = msg["meta"] + "," + msg["source"]+ "," + msg["temperature"] + "," + msg["humidity"] + "," + msg["light"] +
		//			"," + msg["dust"] + "," + msg["airquality_raw"]
		//		//metar := make(map[string]interface{})
		//		tuple := new(xsql.Tuple)
		//		tuple.Message = m
		//
		//		fmt.Println("Etl Join OP OutputController ", tuple)
		//		Oc.Data <- tuple
		//		delete(pp.MsgIdCountMap, message["msgId"].(string))
		//	} else {
		//		pp.MsgIdCountMap[message["msgId"].(string)] = msg
		//	}
		//} else {
		//	msgm := make(map[string]string)
		//	msgm[message["obsType"].(string)] = message["obsVal"].(string)
		//	pp.MsgIdCountMap[message["msgId"].(string)] = msgm
		//	fmt.Println("etl join msgm ", msgm)
		//}




	//default:
	//	return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	//}

	//fmt.Println("AnnotationPlan apply post data is ", tuple)


	//postTime := time.Now().UnixNano()
	//fmt.Println("AnnotationPlan execute time " ,postTime-preTime)

	return tuple
}

func (pp *AnnotationPlan)Prepare()  {
	inputFile,err := os.Open("/home/llh/code/new-kuiper/kuiper/data/city-metadata.txt")
	if err != nil {
		fmt.Println("打开文件出错")
		return
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	pp.AnnotationMap = make(map[string]string)
	for  {
		line,err := inputReader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		strs := strings.SplitN(line, ":", 2)
		pp.AnnotationMap[strs[0]] = strs[1]
	}

}


