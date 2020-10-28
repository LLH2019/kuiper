package extensions

import (
	"container/list"
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"strconv"
)

type KuiperFileSource struct {
	filePath string
	msgId string
	payload string
	sensorId string
	meta string
	obsType string
	obsVal string
	etl *Etl
	list *list.List

	epollList *list.List

}

func (fs *KuiperFileSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing file source")
	return nil
}

func (fs *KuiperFileSource) Configure(device string, props map[string]interface{}) error {
	return nil
}

func (fs *KuiperFileSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {

	//time.Sleep(10000)
	//interval := 100*time.Millisecond
	//fs.epollList = list.New()
	//gtimer.Add(interval, func() {
	//	for e := fs.list.Front(); ; e = e.Next() {
	//		fmt.Println(e.Value)
	//		if e == nil {
	//			e = fs.list.Front()
	//		}
	//		fs.epollList.PushBack(e.Value)
	//	}
	//
	//	//fmt.Println(time.Now(), time.Duration(time.Now().UnixNano() - now.UnixNano()))
	//	//now = time.Now()
	//})

	startId := 1572000000008
	//inputFile,err := os.Open(fs.filePath)
	//if err != nil {
	//	fmt.Println("打开文件出错")
	//	return
	//}
	//defer inputFile.Close()
	//
	//inputReader := bufio.NewReader(inputFile)
	//str2 := "1422748859000,{\"e\":[{\"u\":\"string\",\"n\":\"source\",\"sv\":\"ci4oethyi000302ymejc2wc2j4\"},{\"v\":\"-43.178667\",\"u\":\"lon\",\"n\":\"longitude\"},{\"v\":\"-22.919665\",\"u\":\"lat\",\"n\":\"latitude\"},{\"v\":\"30.6\",\"u\":\"far\",\"n\":\"temperature\"},{\"v\":\"52.3\",\"u\":\"per\",\"n\":\"humidity\"},{\"v\":\"0\",\"u\":\"per\",\"n\":\"light\"},{\"v\":\"65.94\",\"u\":\"per\",\"n\":\"dust\"},{\"v\":\"32\",\"u\":\"per\",\"n\":\"airquality_raw\"}],\"bt\":1422748859000}"
	str2 := "{\"humidity\":5,\"temperature\":12}"
	fs.epollList = list.New()
	fs.epollList.PushBack(str2)
	for   {
		//line,err := inputReader.ReadString('\n')

		//fmt.Println("88888888888888", fs.epollList.Len())
		if fs.epollList.Len() != 0 {

			ele := fs.epollList.Front()
			//fmt.Println(ele)
			//fs.epollList.Remove(ele)
			line := ele.Value.(string)
			//fmt.Println("line:  ", line)
			if len(line) == 0 {
				continue
			}
			//fmt.Println("line:  ", line)
			//handler(fs, line)
			result := make(map[string]interface{})
			meta := make(map[string]interface{})
			fs.msgId = strconv.Itoa(startId)
			startId += 1
			//flatData(fs, &result)
			//fmt.Println("result: ", result)
			fs.payload = line
			result["humidity"] = 5.0
			result["temperature"] = 12.0

			//print("file_source result ", result)
			//str := make(map[string]interface{})
			//str["line"]= line
			fmt.Println("file source post data ", result)
			consumer <- api.NewDefaultSourceTuple(result, meta)
		}
	}
	return

}






