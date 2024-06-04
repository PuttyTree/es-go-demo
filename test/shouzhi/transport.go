package main

import (
	"bytes"
	"encoding/json"
	"es-demo/src/entity"
	"es-demo/src/model"
	"es-demo/src/util"
	"fmt"
	"github.com/valyala/fasthttp"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 迁移全量数据
func transport() {
	client := util.GetFastHttpClient()
	conf := loadConf()
	indices := readIndices()
	if len(indices) < 1 {
		fmt.Println("empty index")
		return
	}
	start := time.Now()
	fmt.Printf("%v,start...\n", start)
	for _, index := range indices {
		transportOneIndex(client, conf, index)
	}
	end := time.Now()
	fmt.Printf("%v,end,duration=%vseconds\n", end, end.Sub(start).Seconds())
}

func transportOneIndex(client *fasthttp.Client, conf *entity.Config, index string) {
	start := time.Now()
	exit := make(chan bool, 1)
	counterChan := make(chan int, 20)
	inChan := make(chan model.Hit, 2000)
	fmt.Printf("index=%v,time=%v,start...\n", index, start)
	go startRead(client, conf, index, inChan)
	go startWrite(client, conf, index, inChan, counterChan, true, nil)
	go counter(counterChan, 0, exit)
	for {
		_, ok := <-exit
		if !ok {
			end := time.Now()
			fmt.Printf("index=%v,duration=%vseconds\n", index, end.Sub(start).Seconds())
			break
		}
	}
}
func startRead(client *fasthttp.Client, conf *entity.Config, index string, hits chan model.Hit) {
	var auth = getAuth("reader")
	if conf.ThreadNum <= 1 {
		doReading(client, auth, conf, index, hits, 0, nil)
	} else {
		var wg sync.WaitGroup
		wg.Add(conf.ThreadNum)
		for i := 0; i < conf.ThreadNum; i++ {
			go doReading(client, auth, conf, index, hits, i, &wg)
		}
		wg.Wait()
	}
	close(hits)
}

func startWrite(client *fasthttp.Client, conf *entity.Config, index string, hit chan model.Hit, counterChan chan int,
	autoClose bool, wg *sync.WaitGroup) {
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	var auth = getAuth("writer")
	if len(auth) > 0 {
		req.Header.Set("Authorization", auth)
	}
	var host = strings.Split(conf.Writer.Endpoint, ",")[0]
	var uri = "http://" + host + "/" + index + "/" + index + "/_bulk"
	req.SetRequestURI(uri)
	var total = 0
	var current = 0
	var buf bytes.Buffer
	var meta string
	for {
		doc, ok := <-hit
		if !ok {
			if current > 0 {
				req.SetBody(buf.Bytes())
				if err := client.Do(req, res); err != nil {
					fmt.Println("req error ", err)
				} else {
					var bulkResponse model.BulkResponse
					json.Unmarshal(res.Body(), &bulkResponse)
					total += len(bulkResponse.Items)
				}
				counterChan <- current
			}
			break
		}
		if len(doc.Routing) > 0 {
			meta = fmt.Sprintf(`{ "index" : { "_id":"%s","_routing":"%s" } } %s`, doc.Id, doc.Routing, "\n")
		} else {
			meta = fmt.Sprintf(`{ "index" : { "_id":"%s" } } %s`, doc.Id, "\n")
		}

		buf.WriteString(meta)
		source := doc.Source.(map[string]interface{})
		nbr := source["trx_crd_nbr"].(string)
		if len(nbr) > 0 {
			split := strings.Split(nbr, "@@@")
			source["trx_nbr"] = split[0]
			if len(split) > 1 {
				source["crd_nbr"] = split[1]
			}
		}

		if source["rmb_amt"] != nil {
			var rmbAmt = 0.0
			switch source["rmb_amt"].(type) {
			case string:
				value, _ := strconv.ParseFloat(source["rmb_amt"].(string), 64)
				rmbAmt = float64(value)
			case float32:
				rmbAmt = float64(source["rmb_amt"].(float32))
			case float64:
				rmbAmt = float64(source["rmb_amt"].(float64))
			}
			if rmbAmt < 0 {
				source["rmb_amt"] = math.Abs(rmbAmt)
			}

		}
		if source["f4"] == nil || len(source["f4"].(string)) < 1 {
			source["f4"] = "0"
		}
		srcBytes, _ := json.Marshal(source)
		buf.WriteString(string(srcBytes))
		buf.WriteString("\n")
		current++
		if current >= conf.BatchSize {
			req.SetBody(buf.Bytes())
			if err := client.Do(req, res); err != nil {
				fmt.Println("req error ", err)
			} else {
				var bulkResponse model.BulkResponse
				json.Unmarshal(res.Body(), &bulkResponse)
				total += len(bulkResponse.Items)
			}
			buf.Reset()
			counterChan <- current
			current = 0
		}
	}
	fmt.Printf("targetIndex=%v,write total=%v\n", index, total)
	if autoClose {
		close(counterChan)
	}
	if wg != nil {
		wg.Done()
	}
}

func doReading(client *fasthttp.Client, auth string, conf *entity.Config, index string, hits chan model.Hit, sliceId int,
	wg *sync.WaitGroup) {
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	if len(auth) > 0 {
		req.Header.Set("Authorization", auth)
	}
	var host = strings.Split(conf.Reader.Endpoint, ",")[0]
	var uri = "http://" + host + "/" + index + "/" + index + "/_search?scroll=1m"
	req.SetRequestURI(uri)

	var body map[string]interface{}
	if conf.ThreadNum <= 1 {
		body = map[string]interface{}{
			"size": conf.BatchSize,
		}
	} else {
		slice := map[string]interface{}{
			"id":  sliceId,
			"max": conf.ThreadNum,
		}
		body = map[string]interface{}{
			"size":  conf.BatchSize,
			"slice": slice,
		}
	}
	marshal, _ := json.Marshal(body)
	req.SetBodyRaw(marshal)
	client.Do(req, res)
	var searchResponse model.SearchResponse
	json.Unmarshal(res.Body(), &searchResponse)

	total := searchResponse.Hits.Total
	fmt.Printf("sourceIndex=%v,sliceId=%v,total=%v\n", index, sliceId, total)
	for _, v := range searchResponse.Hits.Hits {
		hits <- v
	}
	current := len(searchResponse.Hits.Hits)

	for {
		uri = "http://" + host + "/_search/scroll"
		req.SetRequestURI(uri)
		req.Header.SetMethod(fasthttp.MethodPost)
		body = map[string]interface{}{
			"scroll":    "2m",
			"scroll_id": searchResponse.ScrollId,
		}
		marshal, _ := json.Marshal(body)
		req.SetBodyRaw(marshal)
		client.Do(req, res)
		json.Unmarshal(res.Body(), &searchResponse)
		count := len(searchResponse.Hits.Hits)
		current += count
		if count > 0 {
			for _, v := range searchResponse.Hits.Hits {
				hits <- v
			}
		}
		if count < 1 {
			break
		}
	}
	clearScroll(client, host, searchResponse.ScrollId)
	if wg != nil {
		wg.Done()
	}
}

func clearScroll(client *fasthttp.Client, host, scrollId string) {
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(fasthttp.MethodDelete)
	uri := host + "/_search/scroll"
	req.SetRequestURI(uri)
	body := map[string]interface{}{
		"scroll_id": scrollId,
	}
	marshal, _ := json.Marshal(body)
	req.SetBodyRaw(marshal)
	client.Do(req, res)
}

func counter(counterChan chan int, counter int, exit chan bool) {
	for {
		c, ok := <-counterChan
		if !ok {
			fmt.Printf("process:%v\n", counter)
			break
		}
		counter += c
		if counter%100000 == 0 {
			fmt.Printf("process:%v\n", counter)
		}
	}
	exit <- true
	close(exit)
}
