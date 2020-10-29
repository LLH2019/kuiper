package extensions

import (
	"bufio"
	"container/list"
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/gogf/gf/os/gtimer"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type FileSource struct {
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

	poll chan string

}

func (fs *FileSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing file source")
	return nil
}

func (fs *FileSource) Configure(device string, props map[string]interface{}) error {
	fs.filePath ="/home/llh/code/new-kuiper/kuiper/data/SYS_sample_data_senml.csv"
	//fs.filePath = "/home/llh/out/sys_data.txt"

	inputFile,err := os.Open(fs.filePath)
	if err != nil {
		fmt.Println("打开文件出错")
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	fs.list = list.New()
	for  {
		line,err := inputReader.ReadString('\n')
		line = strings.Replace(line, "sv", "v", -1)
		//fs.list = list.New()
		if line != "\n" && line != "" {
			//fmt.Println("222222222", line)
			fs.list.PushBack(line)
		}
		if err != nil {
			break
		}
	}
	//fmt.Println(fs.list)

	fs.poll = make(chan string, 204800)

	//fs.list = list.New()
	//fs.epollList = list.New()
	go func() {
	//str1 := "1422748859000,{\"e\":[{\"u\":\"string\",\"n\":\"source\",\"sv\":\"ci4lr75vd000b02ypdlm6qbly8\"},{\"v\":\"6.13995\",\"u\":\"lon\",\"n\":\"longitude\"},{\"v\":\"46.220411\",\"u\":\"lat\",\"n\":\"latitude\"},{\"v\":\"4.7\",\"u\":\"far\",\"n\":\"temperature\"},{\"v\":\"39.6\",\"u\":\"per\",\"n\":\"humidity\"},{\"v\":\"0\",\"u\":\"per\",\"n\":\"light\"},{\"v\":\"65.07\",\"u\":\"per\",\"n\":\"dust\"},{\"v\":\"17\",\"u\":\"per\",\"n\":\"airquality_raw\"}],\"bt\":1422748859000}"
	//str2 := "1422748859000,{\"e\":[{\"u\":\"string\",\"n\":\"source\",\"sv\":\"ci4oethyi000302ymejc2wc2j4\"},{\"v\":\"-43.178667\",\"u\":\"lon\",\"n\":\"longitude\"},{\"v\":\"-22.919665\",\"u\":\"lat\",\"n\":\"latitude\"},{\"v\":\"30.6\",\"u\":\"far\",\"n\":\"temperature\"},{\"v\":\"52.3\",\"u\":\"per\",\"n\":\"humidity\"},{\"v\":\"0\",\"u\":\"per\",\"n\":\"light\"},{\"v\":\"65.94\",\"u\":\"per\",\"n\":\"dust\"},{\"v\":\"32\",\"u\":\"per\",\"n\":\"airquality_raw\"}],\"bt\":1422748859000}"


	//fs.list.PushBack(str1)
	//fs.list.PushBack(str2)
	//fmt.Println("6666666666",fs.list.Len())

	//time.Sleep(10000)
	interval := 1000*time.Millisecond

	e := fs.list.Front()
	gtimer.Add(interval, func() {
		total := 0
		for i := 0; i<1000; i++ {
		//for i:=fs.list.Front(); i != nil; i=i.Next() {
			//fs.poll <- str1
			//fs.poll <- str2
			total++
			if e == nil {
				e = fs.list.Front()
			}

			fs.poll <- e.Value.(string)
			e = e.Next()
			//fmt.Println(time.Now(), i.Value.(string))
			//if fs.list.Len() == 0 {
			//	time.Sleep(1000)
			//}
			//
			//fmt.Println("111111111111111111")
			//fmt.Println("fmt epolllist front " , fs.list.Len())
			//fs.epollList.PushBack(str1)
			//fs.epollList.PushBack(str2)
			//for e := fs.list.Front(); ; e = e.Next() {
			//fmt.Println(e.Value)
			//if e == nil {
			//	//e = fs.list.Front()
			//	break
			//} else {
			//	fs.epollList.PushBack(str1)
			//
			//}
			//}
		}
		fmt.Println(time.Now(), + total)

		//fmt.Println(time.Now(), time.Duration(time.Now().UnixNano() - now.UnixNano()))
		//now = time.Now()
	})

	}()


	return nil
}

func (fs *FileSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {

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


	//str1 := "1422748859001,{\"e\":[{\"u\":\"string\",\"n\":\"source\",\"sv\":\"ci4lr75o6000002yp6eolj0rm1\"},{\"v\":\"-43.178667\",\"u\":\"lon\",\"n\":\"longitude\"},{\"v\":\"-22.919665\",\"u\":\"lat\",\"n\":\"latitude\"},{\"v\":\"30.6\",\"u\":\"far\",\"n\":\"temperature\"},{\"v\":\"52.3\",\"u\":\"per\",\"n\":\"humidity\"},{\"v\":\"0\",\"u\":\"per\",\"n\":\"light\"},{\"v\":\"65.94\",\"u\":\"per\",\"n\":\"dust\"},{\"v\":\"32\",\"u\":\"per\",\"n\":\"airquality_raw\"}],\"bt\":1422748859000}"
	//str2 := "1422748859000,{\"e\":[{\"u\":\"string\",\"n\":\"source\",\"sv\":\"ci4lr75o6000002yp6eolj0rm25\"},{\"v\":\"-43.178667\",\"u\":\"lon\",\"n\":\"longitude\"},{\"v\":\"-22.919665\",\"u\":\"lat\",\"n\":\"latitude\"},{\"v\":\"30.6\",\"u\":\"far\",\"n\":\"temperature\"},{\"v\":\"52.3\",\"u\":\"per\",\"n\":\"humidity\"},{\"v\":\"0\",\"u\":\"per\",\"n\":\"light\"},{\"v\":\"65.94\",\"u\":\"per\",\"n\":\"dust\"},{\"v\":\"32\",\"u\":\"per\",\"n\":\"airquality_raw\"}],\"bt\":1422748859000}"
	////str2 := "{\"humidity\":5,\"temperature\":12}"
	//fs.epollList = list.New()
	//for i:=0; i<10; i++ {
	//	fs.epollList.PushBack(str1)
	//	fs.epollList.PushBack(str2)
	//}
	//for i:=0; i<1; i++ {
	//	go func() {
			for {
				//line,err := inputReader.ReadString('\n')

				//fmt.Println("88888888888888", fs.epollList.Len())
				//if fs.epollList.Len() != 0 {

					//ele := fs.epollList.Front()
					//fmt.Println(ele)
					//fs.epollList.Remove(ele)
					//line := ele.Value.(string)
					line := <- fs.poll
					//fmt.Println("line:  ", line)
					if len(line) == 0 {
						continue
					}
					line = strings.Replace(line, "sv", "v", -1)
					//fmt.Println("line:  ", line)
					handler(fs, line)
					result := make(map[string]interface{})
					meta := make(map[string]interface{})
					fs.msgId = strconv.Itoa(startId)
					startId += 1
					//flatData(fs, &result)
					//fmt.Println("result: ", result)
					result["MSGID"] = fs.msgId
					result["PAYLOAD"] = fs.payload
					result["TIMESTAMP"] = time.Now().Unix()
					result["SPOUTTIMESTAMP"] = time.Now().Unix()
					result["CHAINSTAMP"] = time.Now().Unix()
					//print("file_source result ", result)
					//str := make(map[string]interface{})
					//str["line"]= line
					//fmt.Println("file source post data ", result)

					consumer <- api.NewDefaultSourceTuple(result, meta)
					runtime.Gosched()
				//}

		//	}
		//}()
	}
	return

}

func flatData(fs *FileSource, m *map[string]interface{}) {
	etl := fs.etl
	//fmt.Println("etl data is ", etl)
	//(*m)["sensorId"] = etl.Bt
	e := etl
	//(*m)["source"] = e.source.sv
	params := e.Parameters
	for i:=0; i<8; i++ {
		(*m)[params[i].Name] = params[i].Value
	}
}

func handler(fs *FileSource, line string) {
	strs := strings.SplitN(line, ",", 2)
	if len(strs) != 2 {
		return
	}
	//fmt.Println("strs ", strs)
	//fmt.Println("strs.length: ", len(strs))
	//fs.msgId = strs[0]
	fs.payload = strs[1]
	//fmt.Println("str: ", str)
	//etl := Etl{}
	//err := json.Unmarshal([]byte(str), &etl)
	//if err != nil {
	//	fmt.Println("解析错误！！！")
	//}
	////fmt.Println("handler etl is ", etl)
	//fs.etl = &etl
}



