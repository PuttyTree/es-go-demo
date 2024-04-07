package main

import (
  "bufio"
  "encoding/json"
  "es-demo/src/model"
  "es-demo/src/util"
  "fmt"
  "github.com/valyala/fasthttp"
  "os"
  "sync"
  "time"
)

var host = "http://localhost:9200"
var index = "test"
var typ = "_doc"
var numberRoutines = 1
var batchCount = 10000

func main() {
  var path = "d://1.json"
  client := util.GetFastHttpClient()
  start := time.Now()
  exit := make(chan bool, 1)
  counterChan := make(chan int, 20)
  inChan := make(chan []model.Hit, 20000)
  go startRead(client, inChan, counterChan)
  go startWrite(path, inChan, exit, counterChan)
  count(counterChan, 0)
  for {
    _, ok := <-exit
    if !ok {
      end := time.Now()
      fmt.Printf("duration=%vseconds\n", end.Sub(start).Seconds())
      break
    }
  }

}

func startRead(client *fasthttp.Client, hits chan []model.Hit, counterChan chan int) {
  if numberRoutines <= 1 {
    doReading(client, hits, counterChan, 0, nil)
  } else {
    var wg sync.WaitGroup
    wg.Add(numberRoutines)
    for i := 0; i < numberRoutines; i++ {
      go doReading(client, hits, counterChan, i, &wg)
    }
    wg.Wait()
  }
  close(hits)
}

func doReading(client *fasthttp.Client, hits chan []model.Hit, counterChan chan int, sliceId int, wg *sync.WaitGroup) {
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  var uri = host + "/" + index + "/" + typ + "/_search?scroll=1m"
  req.SetRequestURI(uri)
  var body map[string]any
  if numberRoutines <= 1 {
    body = map[string]interface{}{
      "size": batchCount,
    }
  } else {
    slice := map[string]interface{}{
      "id":  sliceId,
      "max": numberRoutines,
    }
    body = map[string]interface{}{
      "size":  batchCount,
      "slice": slice,
    }
  }
  marshal, _ := json.Marshal(body)
  req.SetBodyRaw(marshal)
  client.Do(req, res)
  var searchResponse model.SearchResponse
  json.Unmarshal(res.Body(), &searchResponse)

  total := searchResponse.Hits.Total
  hits <- searchResponse.Hits.Hits
  current := len(searchResponse.Hits.Hits)
  counterChan <- current
  for {
    uri = host + "/_search/scroll"
    req.SetRequestURI(uri)
    req.Header.SetMethod(fasthttp.MethodGet)
    body = map[string]interface{}{
      "scroll":    "1m",
      "scroll_id": searchResponse.ScrollId,
    }
    marshal, _ := json.Marshal(body)
    req.SetBodyRaw(marshal)
    client.Do(req, res)
    json.Unmarshal(res.Body(), &searchResponse)
    count := len(searchResponse.Hits.Hits)
    current += count
    counterChan <- count
    if count > 0 {
      hits <- searchResponse.Hits.Hits
    }
    if count < 1 || current >= total {
      break
    }

  }
  //fmt.Printf("sliceId=%v,total=%v,cur=%v\n", sliceId, total, current)
  clearScroll(client, searchResponse.ScrollId)
  if wg != nil {
    wg.Done()
  }
}

func clearScroll(client *fasthttp.Client, scrollId string) {
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
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

func startWrite(path string, hits chan []model.Hit, exit chan bool, counterChan chan int) {
  var file *os.File
  _, err := os.Stat(path)
  /*if os.IsNotExist(err) {
     file, _ = os.Create(path)
    } else {
     file, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
   }*/
  if err == nil {
    os.Remove(path)
  }
  file, _ = os.Create(path)
  defer file.Close()
  w := bufio.NewWriter(file)
  writer := bufio.NewWriterSize(w, 4096*10)
  index := 0
  for {
    docs, ok := <-hits
    if !ok {
      if len(docs) > 0 {
        for _, doc := range docs {
          bytes, _ := json.Marshal(doc.Source)
          writer.WriteString(string(bytes) + "\n")
          writer.Flush()
        }
      }
      break
    }
    for _, doc := range docs {
      bytes, _ := json.Marshal(doc.Source)
      writer.WriteString(string(bytes) + "\n")
      index++
      if index%1000 == 0 {
        writer.Flush()
      }
    }

  }
  writer.Flush()
  close(counterChan)
  exit <- true
  close(exit)
}

func count(counterChan chan int, counter int) {
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
}
