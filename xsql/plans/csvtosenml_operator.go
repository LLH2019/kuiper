package plans

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type CsvToSenMLPlan struct {
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
func (pp *CsvToSenMLPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	fmt.Println("AnnotationPlan apply pre data is ", data, reflect.TypeOf(data))
	var results []map[string]interface{}
	result := make(map[string]interface{})
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		message := input.Message

		jsonStr := pp.convertToJson(message)
		//source := strings.Split(message["obsVal"].(string), ",")[0]
		//
		//
		//line := pp.annotationMap[source]
		//message["obsVal"] = message["obsVal"].(string) + "," + line
		//tuple := new(xsql.Tuple)
		//tuple.Message = message
		fmt.Println("CsvToSenML tuple data ", jsonStr)
		result["demo"] = jsonStr
		results = append(results, result)

		fmt.Println("CsvToSenML tuple data ", results)
		Oc.Data <- jsonStr


	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}

	fmt.Println("CsvToSenML apply post data is ", results)

	if ret, err := json.Marshal(results); err == nil {
		return ret
	} else {
		return fmt.Errorf("run Select error: %v", err)
	}
}

func (pp *CsvToSenMLPlan)Prepare()  {
	inputFile,err := os.Open("/home/llh/code/new-kuiper/kuiper/data/city-metadata.txt")
	if err != nil {
		fmt.Println("打开文件出错")
		return
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	pp.annotationMap = make(map[string]string)
	for  {
		line,err := inputReader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		strs := strings.SplitN(line, ":", 2)
		pp.annotationMap[strs[0]] = strs[1]
	}

}

func (pp *CsvToSenMLPlan) convertToJson(message xsql.Message) interface{} {
	obsVal := message["obsVal"].(string)
	obsVal = strings.Replace(obsVal, "\n", "", -1)
	strs := strings.Split(obsVal, ",")

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
	t := strconv.FormatInt(time.Now().Unix(), 10)
	//jsonStr := "[" +  "\"demo\":" + "{" + "\"bt\":\"" + strs[1] + "\"," + "\"e\":" + e + ",\"time\":" + t + "}" +"]"
	jsonStr := "{" + "\"bt\":\"" + strs[1] + "\"," + "\"e\":" + e + ",\"time\":" + t + "}"
	return jsonStr
}