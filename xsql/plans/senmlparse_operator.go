package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
	"github.com/emqx/kuiper/xstream/nodes"
	"reflect"
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
}

/**
 *  input: *xsql.Tuple
 *  output: *xsql.Tuple
 */
func (pp *SenMLParsePlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	log := ctx.GetLogger()
	log.Debugf("SenMLParse plan receive %s", data)
	fmt.Println("SenMLParsePlan apply pre data is ", data)
	var results []map[string]interface{}

	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		ve := pp.getVE(input, input, fv, afv)
		//fmt.Println("vvvvve ", ve, reflect.TypeOf(ve))
		if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta && input.Metadata != nil {
				r[common.MetaKey] = input.Metadata
			}
			results = append(results, r)
		}
	}

	fmt.Println("senMlParse OP result", results)

	if len(results) == 0 {
		return nil
	}
	var procData = results[0]
	fmt.Println(procData)

	handler(pp, procData["payload"])

	flatData(pp, &procData)
	etl := pp.etl

	var tuples [] *xsql.Tuple

	preMap := make(map[string]interface{})
	for i:=0 ; i<8; i++  {
		preMap[etl.Parameters[i].Name] = etl.Parameters[i].Value
	}

	meta := strconv.FormatFloat(etl.Bt.(float64), 'E', -1,64 )+ "," + preMap["longitude"].(string) + "," + preMap["latitude"].(string)
	sensorId := preMap["source"]

	for k,v := range(preMap) {
		if k != "longitude" && k != "latitude" {
			m := make(map[string]interface{})
			m["msgId"] = procData["msgId"]
			m["sensorId"] = sensorId
			m["meta"] = meta
			m["obsVal"] = v
			m["obsType"] = k
			//metar := make(map[string]interface{})
			tuple := new(xsql.Tuple)
			tuple.Message = m
			fmt.Println("senMl aaaaaaaaaaaaaaaaaaa", tuple)
			Oc.Data <- tuple
			tuples = append(tuples, tuple)
		}
	}

	fmt.Println("SenMLParsePlan apply post data is ", tuples, " type", reflect.TypeOf(tuples))

	//Oc = &nodes.OutputController{Data: make(chan interface{},1024)}
	//Oc.Input(tuples)
	return tuples
}


func (pp *SenMLParsePlan) getVE(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	} else {
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
	}
}

func flatData(pp *SenMLParsePlan, m *map[string]interface{}) {
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

func handler(pp *SenMLParsePlan, line interface{}) {
	//strs := strings.SplitN(line, ",", 2)
	//if len(strs) != 2 {
	//	return
	//}
	////fmt.Println("strs.length: ", len(strs))
	//str := strs[0]
	//fs.payload = strs[1]
	fmt.Println("str: ", line)
	etl := extensions.Etl{}
	err := json.Unmarshal([]byte(line.(string)), &etl)
	if err != nil {
		fmt.Println("解析错误！！！")
	}
	//fmt.Println("handler etl is ", etl)
	pp.etl = &etl
}

func (pp *SenMLParsePlan)Emit(i interface{}) interface{} {
	return pp.Emit(i)
}

func (p *SenMLParsePlan) Prepare(){

}