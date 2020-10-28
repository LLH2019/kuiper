package plans

import (
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"strconv"
	"strings"
	"time"
)

type CsvToSenMLWithoutPlan struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool
	MaxCountPossible int
	MsgIdCountMap map[string]map[string]string
	annotationMap map[string]string
}



/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *CsvToSenMLWithoutPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {

	//preTime := time.Now().UnixNano()

	//log := ctx.GetLogger()
	//log.Debugf("project plan receive %s", data)
	//fmt.Println("CsvToSenMLPlan apply pre data is ", data, reflect.TypeOf(data))
	//var results []map[string]interface{}
	//result := make(map[string]interface{})
	//switch input := data.(type) {
	//case error:
	//	return input
	//case *xsql.Tuple:
		message := data.(*xsql.Tuple).Message

		jsonStr := pp.convertToJson(message)
		//source := strings.Split(message["obsVal"].(string), ",")[0]
		//
		//
		//line := pp.annotationMap[source]
		//message["obsVal"] = message["obsVal"].(string) + "," + line
		//tuple := new(xsql.Tuple)
		//tuple.Message = message
		//fmt.Println("CsvToSenML tuple data ", jsonStr)
		//result["demo"] = jsonStr
		//results = append(results, result)

		//fmt.Println("CsvToSenML tuple data ", results)
		Oc.Data <- jsonStr


	//default:
	//	return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	//}

	//fmt.Println("2222222222222222222222222222222")
	//fmt.Println("CsvToSenML apply post data is ", results)

	//if ret, err := json.Marshal(results); err == nil {
	//	return ret
	//} else {
	//	return fmt.Errorf("run Select error: %v", err)
	//}
	//return results

	//postTime := time.Now().UnixNano()
	//fmt.Println("CsvToSenMLWithoutPlan execute time " ,postTime-preTime)
	return nil
}

func (pp *CsvToSenMLWithoutPlan)Prepare()  {
	//inputFile,err := os.Open("/home/llh/code/new-kuiper/kuiper/data/city-metadata.txt")
	//if err != nil {
	//	fmt.Println("打开文件出错")
	//	return
	//}
	//defer inputFile.Close()
	//inputReader := bufio.NewReader(inputFile)
	//pp.annotationMap = make(map[string]string)
	//for  {
	//	line,err := inputReader.ReadString('\n')
	//	if err != nil {
	//		fmt.Println(err)
	//		break
	//	}
	//	strs := strings.SplitN(line, ":", 2)
	//	pp.annotationMap[strs[0]] = strs[1]
	//}

}

func (pp *CsvToSenMLWithoutPlan) convertToJson(message xsql.Message) interface{} {
	//fmt.Println("csvTo message", message)
	//obsType := message["obsType"].(string)
	obsVal := message["obsVal"].(string)
	//fmt.Println("----------", obsVal)
	//meta := message["meta"].(string)
	//msgId := message["msgId"].(string)
	obsVal = strings.Replace(obsVal, "\n", "", -1)
	strs := strings.Split(obsVal, ",")
	//o1 := "{" + "\"sv\":\"" + obsType + "\","  +"\"u\":\"string\"," + "\"n\":\"type\""+ "}"
	//o2 := "{" + "\"sv\":\"" + meta + "\","  +"\"u\":\"string\"," + "\"n\":\"val\""+ "}"

	e1 := "{" + "\"sv\":\"" + strs[0] + "\","  +"\"u\":\"string\"," + "\"n\":\"source\""+ "}"
 	e2 := "{" + "\"v\":\"" + strs[2] + "\","  +"\"u\":\"lon\"," + "\"n\":\"longitude\""+ "}"
	e3 := "{" + "\"v\":\"" + strs[3] + "\","  +"\"u\":\"lat\"," + "\"n\":\"latitude\""+ "}"
	e4 := "{" + "\"v\":\"" + strs[4] + "\","  +"\"u\":\"far\"," + "\"n\":\"temperature\""+ "}"
	e5 := "{" + "\"v\":\"" + strs[5] + "\","  +"\"u\":\"per\"," + "\"n\":\"humidity\""+ "}"
	e6 := "{" + "\"v\":\"" + strs[6] + "\","  +"\"u\":\"per\"," + "\"n\":\"light\""+ "}"
	e7 := "{" + "\"v\":\"" + strs[7] + "\","  +"\"u\":\"per\"," + "\"n\":\"dust\""+ "}"
	e8 := "{" + "\"v\":\"" + strs[8] + "\","  +"\"u\":\"per\"," + "\"n\":\"airquality_raw\""+ "}"
	//e9 := "{" + "\"sv\":\"" + strs[9] + "\","  +"\"u\":\"string\"," + "\"n\":\"location\""+ "}"
	//e10 := "{" + "\"sv\":\"" + strs[10] + "\","  +"\"u\":\"string\"," + "\"n\":\"type\""+ "}"


	e := "[" + e1 + "," + e2 + "," + e3 + "," + e4 + "," + e5 + "," + e6 + "," + e7 + "," + e8 +  "]"

	//e := "[" + e1 + "," + e2 + "," + e3 + "," + e4 + "," + e5 + "," + e6 + "," + e7 + "," + e8 + "," + e9 + "," + e10 + "]"

	//e := "[" +  o1 + "," + o2 +  "]"
	t := strconv.FormatInt(time.Now().Unix(), 10)
	jsonStr := "[" +  "\"demo\":" + "{" + "\"bt\":\"" + strs[1] + "\"," + "\"e\":" + e + ",\"time\":" + t + "}" +"]"
	//jsonStr := "{" + "\"msgid\":\"" + msgId + "\"," + "\"e\":" + e + ",\"time\":" + t + "}"
	return jsonStr
}