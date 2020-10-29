package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
	"github.com/emqx/kuiper/xstream/nodes"
	"strconv"
)

type SenMLParsePlan struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool

	msgId string
	sensorId string
	meta string
	obsType string
	obsVal string
	bt string
	etl *extensions.Etl
	//oc *nodes.OutputController
	//PreMap  map[string]interface{}
}

/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *SenMLParsePlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	//preTime := time.Now().UnixNano()

	//var t1, t2,t3,t4,t5 int64
	//log := ctx.GetLogger()
	//log.Debugf("SenMLParse plan receive %s", data)

	//t1 = time.Now().UnixNano()
	//fmt.Println("SenMLParsePlan apply pre data is ", data)
	//var results []map[string]interface{}

	//switch input := data.(type) {
	//case error:
	//	return input
	//case *xsql.Tuple:
	input := data.(*xsql.Tuple)
	//fmt.Println("SenMLParsePlan input data  is ", input.Message)
	//	ve := pp.getVE(input, input, fv, afv)
	//	//fmt.Println("vvvvve ", ve, reflect.TypeOf(ve))
	//	if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
	//		return fmt.Errorf("run Select error: %s", err)
	//	} else {
	//		if pp.SendMeta && input.Metadata != nil {
	//			r[common.MetaKey] = input.Metadata
	//		}
	//		results = append(results, r)
	//	}
	////}
	//t2 = time.Now().UnixNano()
	//fmt.Println("senMlParse OP result", results)
	//
	//if len(results) == 0 {
	//	return nil
	//}
	//var procData = results[0]
	//fmt.Println("procData" , input.Message)

	//handler(pp, input.Message["payload"])
	etl := extensions.Etl{}
	err := json.Unmarshal([]byte(input.Message["PAYLOAD"].(string)), &etl)
	if err != nil {
		fmt.Println("解析错误！！！")
	}
	//fmt.Println("handler etl is ", etl)
	pp.etl = &etl

	//t3 = time.Now().UnixNano()
	flatData(pp, &input.Message)
	//t4 = time.Now().UnixNano()
	//etl := pp.etl

	//var tuples [] *xsql.Tuple

	preMap := make(map[string]interface{})
	for i:=0 ; i<8; i++  {
		preMap[etl.Parameters[i].Name] = etl.Parameters[i].Value
	}

	meta := strconv.FormatFloat(etl.Bt.(float64), 'f', -1,64 )+ "," + preMap["longitude"].(string) + "," + preMap["latitude"].(string)
	//meta :=  etl.Bt.(string) + "," + preMap["longitude"].(string) + "," + preMap["latitude"].(string)
	//fmt.Println("senml------------", meta)
	sensorId := preMap["source"]

	//t5 = time.Now().UnixNano()
	for k,v := range(preMap) {
		if k != "longitude" && k != "latitude" {
			m := make(map[string]interface{})
			m["MSGID"] = input.Message["MSGID"]
			m["SENSORID"] = sensorId
			m["META"] = meta
			m["OBSTYPE"] = k
			m["OBSVAL"] = v
			m["op"] = "SENML"
			m["TIMESTAMP"] = input.Message["TIMESTAMP"]
			m["SPOUTTIMESTAMP"] = input.Message["SPOUTTIMESTAMP"]
			m["CHAINSTAMP"] = input.Message["CHAINSTAMP"]
			//metar := make(map[string]interface{})
			tuple := new(xsql.Tuple)
			tuple.Message = m
			//fmt.Println("senMl aaaaaaaaaaaaaaaaaaa", tuple)
			Oc.Data <- tuple
			//pp.
			//tuples = append(tuples, tuple)
		}
	}

	//postTime := time.Now().UnixNano()
	//fmt.Println("SenMLParsePlan execute time " , t1-preTime, t2-t1, t3-t2, t4-t3, t5-t4, postTime-preTime)
	//fmt.Println("SenMLParsePlan execute time " ,postTime-preTime)
	//fmt.Println("SenMLParsePlan apply post data is ", tuples, " type", reflect.TypeOf(tuples))

	//Oc = &nodes.OutputController{Data: make(chan interface{},1024)}
	//Oc.Input(tuples)
	//return tuples
	return nil
}


//func (pp *SenMLParsePlan) getVE(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
//	afv.SetData(agg)
//	if pp.IsAggregate {
//		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
//	} else {
//		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
//	}
//}

func flatData(pp *SenMLParsePlan, m *xsql.Message) {
	etl := pp.etl
	//fmt.Println("etl data is ", etl)
	(*m)["sensorId"] = etl.Bt
	e := etl
	//(*m)["source"] = e.source.sv
	params := e.Parameters
	for i:=0; i<8; i++ {
		(*m)[params[i].Name] = params[i].Value
	}
}

//func handler(pp *SenMLParsePlan, line interface{}) {
//	//strs := strings.SplitN(line, ",", 2)
//	//if len(strs) != 2 {
//	//	return
//	//}
//	////fmt.Println("strs.length: ", len(strs))
//	//str := strs[0]
//	//fs.payload = strs[1]
//	//fmt.Println("str: ", line)
//	etl := extensions.Etl{}
//	err := json.Unmarshal([]byte(line.(string)), &etl)
//	if err != nil {
//		fmt.Println("解析错误！！！")
//	}
//	//fmt.Println("handler etl is ", etl)
//	pp.etl = &etl
//}

func (pp *SenMLParsePlan)Emit(i interface{}) interface{} {
	return pp.Emit(i)
}

func (p *SenMLParsePlan) Prepare(){

}