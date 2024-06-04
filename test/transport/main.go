package main

import (
  "bufio"
  "bytes"
  "encoding/base64"
  "encoding/json"
  "es-demo/src/entity"
  "es-demo/src/model"
  "es-demo/src/util"
  "fmt"
  "github.com/valyala/fasthttp"
  "io"
  "math"
  "os"
  "os/exec"
  "strings"
  "sync"
  "time"
)

//var sourceHost = "http://localhost:9200"
//var sourceIndex = "xxx"
//var sourceType = "_doc"
//var sourceCluster = "cluster1"
//
//var targetHost = "http://localhost:9200"
//var targetIndex = "yyy"
//var targetType = "_doc"
//var targetCluster = "cluster2"

//var numberRoutines = 1
//var batchCount = 1000

var Conf *entity.Config = nil

const CONF1 = "conf.json"

func main() {
  transportIndex()
}

func transportIndex() {
  client := util.GetFastHttpClient()
  start := time.Now()
  exit := make(chan bool, 1)
  counterChan := make(chan int, 20)
  inChan := make(chan model.Hit, 2000)
  conf := loadConf()
  fmt.Printf("sourceIndex=%v,targetIndex=%v\n", conf.Reader.Index, conf.Writer.Index)
  go startRead(client, conf, inChan)
  go startWrite(client, conf, inChan, counterChan, true, nil)
  go counter(counterChan, 0, exit)
  for {
    _, ok := <-exit
    if !ok {
      end := time.Now()
      fmt.Printf("duration=%vseconds\n", end.Sub(start).Seconds())
      break
    }
  }
}

func transportIndicesByFile(path string) {
  file, _ := os.Open(path)
  defer file.Close()
  reader := bufio.NewReaderSize(file, 4096)
  var indices []string
  for {
    line, _, err := reader.ReadLine()
    if len(line) < 1 || err == io.EOF {
      break
    }
    indices = append(indices, string(line))
  }
  var step = 50
  client := util.GetFastHttpClient()
  start := time.Now()
  exit := make(chan bool, 1)
  counterChan := make(chan int, 10000)
  for i := 0; i < len(indices); i = i + step {
    end := math.Min(float64(i+step), float64(len(indices)))
    sub := indices[i:int(end)]
    size := len(sub)
    fmt.Println(sub)
    var wg sync.WaitGroup
    wg.Add(size)
    for _, value := range sub {
      go transportOneIndex(client, &wg, counterChan, value, value, value, value)
    }
    wg.Wait()
  }
  close(counterChan)

  for {
    _, ok := <-exit
    if !ok {
      end := time.Now()
      fmt.Printf("duration=%vseconds\n", end.Sub(start).Seconds())
      break
    }
  }
}

func transportOneIndex(client *fasthttp.Client, wg *sync.WaitGroup, counterChan chan int,
  srcIndex string, srcType string, tarIndex string, tarType string) {
  //inChan := make(chan model.Hit, 2000)

  //go startRead(client, srcIndex, srcType, inChan)
  //go startWrite(client, tarIndex, tarType, inChan, counterChan, false, wg)
}
func startRead(client *fasthttp.Client, conf *entity.Config, hits chan model.Hit) {
  var auth = getAuth("reader")
  if conf.ThreadNum <= 1 {
    doReading(client, auth, conf, hits, 0, nil)
  } else {
    var wg sync.WaitGroup
    wg.Add(conf.ThreadNum)
    for i := 0; i < conf.ThreadNum; i++ {
      go doReading(client, auth, conf, hits, i, &wg)
    }
    wg.Wait()
  }
  close(hits)
}

func startWrite(client *fasthttp.Client, conf *entity.Config, hit chan model.Hit, counterChan chan int,
  autoClose bool, wg *sync.WaitGroup) {
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  var auth = getAuth("writer")
  if len(auth) > 0 {
    req.Header.Set("Authorization", auth)
  }
  var host = strings.Split(conf.Writer.Endpoint, ",")[0]
  var uri = "http://" + host + "/" + conf.Writer.Index + "/" + conf.Writer.Type + "/_bulk"
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
    srcBytes, _ := json.Marshal(doc.Source)
    //buf.WriteString(string(srcBytes))
    buf.Write(srcBytes)
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
  fmt.Printf("targetIndex=%v,total=%v\n", conf.Writer.Index, total)
  if autoClose {
    close(counterChan)
  }
  if wg != nil {
    wg.Done()
  }
}

func doReading(client *fasthttp.Client, auth string, conf *entity.Config, hits chan model.Hit, sliceId int,
  wg *sync.WaitGroup) {
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  if len(auth) > 0 {
    req.Header.Set("Authorization", auth)
  }
  var host = strings.Split(conf.Reader.Endpoint, ",")[0]
  var uri = "http://" + host + "/" + conf.Reader.Index + "/" + conf.Reader.Type + "/_search?scroll=1m"
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
  fmt.Printf("sourceIndex=%v,sliceId=%v,total=%v\n", conf.Reader.Index, sliceId, total)
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
func loadConf() *entity.Config {
  if Conf != nil {
    return Conf
  }
  confFile := getArg("-c=")
  if len(confFile) < 1 {
    curr, _ := exec.LookPath(os.Args[0])
    confFile = curr + string(os.PathSeparator) + CONF1
  }
  fmt.Printf("confFile=%v\n", confFile)
  content, _ := os.ReadFile(confFile)
  json.Unmarshal(content, &Conf)
  return Conf
}

func getArg(key string) string {
  if len(os.Args) < 1 {
    return ""
  }
  var value = ""
  for _, v := range os.Args {
    if strings.Contains(v, key) {
      value = strings.Split(v, key)[1]
      break
    }
  }
  return value
}

func getAuth(point string) string {
  if Conf == nil {
    Conf = loadConf()
  }
  if point == "reader" {
    if len(Conf.Reader.AccessId) < 1 {
      return ""
    } else {
      s := Conf.Reader.AccessId + ":" + Conf.Reader.AccessKey
      return "Basic " + base64.StdEncoding.EncodeToString([]byte(s))
    }
  } else {
    if len(Conf.Writer.AccessId) < 1 {
      return ""
    } else {
      s := Conf.Writer.AccessId + ":" + Conf.Writer.AccessKey
      return "Basic " + base64.StdEncoding.EncodeToString([]byte(s))
    }
  }
}
