package plans

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"reflect"
)

type RangeFilterPlan struct {
	Key string
	Min float64
	Max float64
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *RangeFilterPlan) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *nodes.OutputController) interface{} {
	log := ctx.GetLogger()
	log.Debugf("filter plan receive %s", data)
	//fmt.Println("range filter pre data is : ", data , "type is:", reflect.TypeOf(data))
	tuple := data.(*xsql.Tuple)
	msg := tuple.Message
	//fmt.Println("msg data is : ", msg, "type is ", reflect.TypeOf(msg))
	value := msg[p.Key]
	//fmt.Println("value is: ", value, "error is", err)
	//if err {
	//	fmt.Println("range filter result not change data is : ", data , "type is:", reflect.TypeOf(data))
	//	return tuple
	//}
	//min := (*p).min
	//max := (*p).max
	if value.(float64) >= (*p).Min && value.(float64) <= (*p).Max{
		fmt.Println("range filter true result data is : ", data , "type is:", reflect.TypeOf(data))
		return tuple
	}

	//fmt.Println("range filter result nil data is : ", data , "type is:", reflect.TypeOf(data))
	return nil
}

func (p *RangeFilterPlan) Prepare(){

}
