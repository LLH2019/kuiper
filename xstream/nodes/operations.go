package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"sync"
)

type OutputController struct {
	Data chan interface{}
}

func (oc *OutputController)Input(i chan interface{})  {
	oc.Data <- i
}

func (oc *OutputController)Output() chan interface{} {
	return oc.Data
}

// UnOperation interface represents unary operations (i.e. Map, Filter, etc)
type UnOperation interface {
	Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, Oc *OutputController) interface{}
	Prepare()
	//Prepare()
}

// UnFunc implements UnOperation as type func (context.Context, interface{})
type UnFunc func(api.StreamContext, interface{}) interface{}

// Apply implements UnOperation.Apply method
func (f UnFunc) Apply(ctx api.StreamContext, data interface{}) interface{} {
	return f(ctx, data)
}

type UnaryOperator struct {
	*defaultSinkNode
	op        UnOperation
	mutex     sync.RWMutex
	cancelled bool
	Oc *OutputController
}

// NewUnary creates *UnaryOperator value
func New(name string, bufferLength int) *UnaryOperator {
	return &UnaryOperator{
		defaultSinkNode: &defaultSinkNode{
			input: make(chan interface{}, bufferLength),
			defaultNode: &defaultNode{
				name:        name,
				outputs:     make(map[string]chan<- interface{}),
				concurrency: 1,
			},
		},
		Oc: &OutputController{Data: make(chan interface{}, bufferLength)},
	}
}

// SetOperation sets the executor operation
func (o *UnaryOperator) SetOperation(op UnOperation) {
	o.op = op
}

// SetOperation sets the executor operation
func (o *UnaryOperator) GetOperation() UnOperation {
	return o.op
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("Unary operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		fmt.Println("errCh exec 555555555555")
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}

	// validate p
	if o.concurrency < 1 {
		o.concurrency = 1
	}
	//reset status
	o.statManagers = nil

	for i := 0; i < o.concurrency; i++ { // workers
		o.op.Prepare()
		instance := i
		go o.doOp(ctx.WithInstance(instance), errCh)
	}
}

func (o *UnaryOperator) doOp(ctx api.StreamContext, errCh chan<- error) {
	logger := ctx.GetLogger()
	if o.op == nil {
		logger.Infoln("Unary operator missing operation")
		return
	}
	exeCtx, cancel := ctx.WithCancel()

	defer func() {
		logger.Infof("unary operator %s instance %d done, cancelling future items", o.name, ctx.GetInstanceId())
		cancel()
	}()

	stats, err := NewStatManager("op", ctx)
	if err != nil {
		select {
		case errCh <- err:
			fmt.Println("doOp error77777777777777777")
			logger.Errorf("unary operator %s error %s", o.name, err)
		case <-ctx.Done():
			logger.Infof("unary operator %s cancelling....", o.name)
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
		}
		return
	}
	o.mutex.Lock()
	o.statManagers = append(o.statManagers, stats)
	o.mutex.Unlock()
	fv, afv := xsql.NewFunctionValuersForOp(exeCtx)
	//for i:=0; i<1; i++ {
	//	go func() {
			for {
				select {
				// process incoming item
				case item := <-o.input:
					//fmt.Println("666666666666--item", item)
					processed := false
					if item, processed = o.preprocess(item); processed {
						break
					}
					stats.IncTotalRecordsIn()
					stats.ProcessTimeStart()

					//fmt.Println("666666666666--item", item)

					o.op.Apply(exeCtx, item, fv, afv, o.Oc)
					//result := o.Oc.Output()

					//go func() {
					for i := 0; i < len(o.Oc.Data); i++ {
						//fmt.Println("bbbbbbbbbbbbbbbbb", result, len(o.Oc.Data))
						//for  {
						val := <- o.Oc.Data
						o.Broadcast(val)
						//fmt.Println("00000000000000", o.name, len(o.input))
						stats.IncTotalExceptions()
						//select {
						//case result := <-o.Oc.Data:
						//
						//	//fmt.Println("77777777777--result", result, reflect.TypeOf(result))
						//	switch val := result.(type) {
						//	case nil:
						//		//continue
						//	case error:
						//		logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val)
						//		o.Broadcast(val)
						//		stats.IncTotalExceptions()
						//		//continue
						//	case []*api.DefaultSourceTuple:
						//		for i := 0; i < len(val); i++ {
						//			fmt.Println("i am No ", i, val[i])
						//			logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val[i])
						//			o.Broadcast(val[i])
						//			stats.IncTotalExceptions()
						//		}
						//		//continue
						//	case []*xsql.Tuple:
						//		for i := 0; i < len(val); i++ {
						//			fmt.Println("i am No ", i, val[i])
						//			logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val[i])
						//			o.Broadcast(val[i])
						//			stats.IncTotalExceptions()
						//		}
						//		//continue
						//	case *xsql.Tuple:
						//		//fmt.Println("i am No ", val)
						//		//logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val)
						//		o.Broadcast(val)
						//		stats.IncTotalExceptions()
						//		//continue
						//	default:
						//		stats.ProcessTimeEnd()
						//		o.Broadcast(val)
						//		stats.IncTotalRecordsOut()
						//		stats.SetBufferLength(int64(len(o.input)))
						//	}
						//}
					}
					//}()

				// is cancelling
				case <-ctx.Done():
					logger.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
					o.mutex.Lock()
					cancel()
					o.cancelled = true
					o.mutex.Unlock()
					return
				}
		//	}
		//}()
	}
}
