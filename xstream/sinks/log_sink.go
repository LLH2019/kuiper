package sinks

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/collectors"
	"sync"
	"time"
)

// log action, no properties now
// example: {"log":{}}
func NewLogSink() *collectors.FuncCollector {
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		log := ctx.GetLogger()
		log.Infof("sink result for rule %s: %s", ctx.GetRuleId(), data)
		return nil
	})
}

type QueryResult struct {
	Results   []string
	LastFetch time.Time
	Mux       sync.Mutex
}

var QR = &QueryResult{LastFetch: time.Now()}

func NewLogSinkToMemory() *collectors.FuncCollector {
	QR.Results = make([]string, 10)
	fmt.Println("new log sink to memory", QR.Results)
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		fmt.Println("inner new log to sink memory " , QR.Results,"---", fmt.Sprintf("%s", data))
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		fmt.Println("inner new log to sink memory " , QR.Results, "---", fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		fmt.Println("inner new log to sink memory " , QR.Results, "---", fmt.Sprintf("%s", data))
		return nil
	})
}
