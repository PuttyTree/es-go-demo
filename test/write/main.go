package main

import (
  "bufio"
  "bytes"
  "es-demo/src/util"
  "fmt"
  "github.com/valyala/fasthttp"
  "io"
  "os"
  "sync"
  "time"
)

var host = "http://localhost:9200"
var index = "logs_index"
var typ = "_doc"

var batchCount = 10000
var numberRoutines = 16

//read from json file, then write to ES
func main() {
  var path = "d://sample_data.json"

  inChan := make(chan string, 12000)
  exit := make(chan bool, 1)
  counterChan := make(chan int, 20)
  client := util.GetFastHttpClient()
  start := time.Now()
  go readTwo(path, inChan)
  go writeTwo(inChan, exit, client, counterChan)
  count(counterChan, 0)
  for {
    _, ok := <-exit
    if !ok {
      end := time.Now()
      fmt.Printf("duration=%v\n", end.Sub(start).Seconds())
      break
    }
  }
}

func count(counterChan chan int, counter int) {
  for {
    c, ok := <-counterChan
    if !ok {
      fmt.Printf("process:%v\n", counter)
      break
    }
    counter += c
    if counter%10000 == 0 {
      fmt.Printf("process:%v\n", counter)
    }
  }
}

func readTwo(path string, inChan chan string) {
  file, err := os.Open(path)
  if err != nil {
    fmt.Println("open file error:", err)
    return
  }
  defer file.Close()
  reader := bufio.NewReaderSize(file, 4096*8)
  for {
    line, _, err := reader.ReadLine()
    if len(line) < 1 || err == io.EOF {
      break
    }
    inChan <- string(line)

  }
  close(inChan)
}

func writeTwo(inChan chan string, exit chan bool, client *fasthttp.Client, counterChan chan int) {
  var wg sync.WaitGroup

  wg.Add(numberRoutines)
  for i := 0; i < numberRoutines; i++ {
    go writeES(inChan, &wg, client, counterChan)
  }
  wg.Wait()
  close(counterChan)
  exit <- true
  close(exit)
}

func writeES(inChan chan string, wg *sync.WaitGroup, client *fasthttp.Client, counterChan chan int) {
  var lines = make([]string, 0, batchCount)
  for {
    line, ok := <-inChan
    if !ok {
      if len(lines) > 0 {
        write(lines, client)
        counterChan <- len(lines)
        lines = make([]string, 0, batchCount)
      }
      wg.Done()
      break
    }
    lines = append(lines, line)
    if len(lines) == cap(lines) {
      write(lines, client)
      counterChan <- len(lines)
      lines = make([]string, 0, batchCount)
    }
  }

}
func readOne(path string) {
  file, err := os.Open(path)
  if err != nil {
    fmt.Println("open file error:", err)
    return
  }
  defer file.Close()

  reader := bufio.NewReader(file)
  var lines = make([]string, 10, 10)
  var index = 0
  var total = 0
  client := util.GetFastHttpClient()
  for {
    line, _, err := reader.ReadLine()
    if total > 30 {
      break
    }
    if len(line) < 1 || err == io.EOF {
      write(lines, client)
      total += index
      fmt.Printf("process:%d\n", total)
      break
    }
    lines = append(lines, string(line))
    index++
    if index == 10 {
      write(lines, client)
      total += index
      fmt.Printf("process:%d\n", total)
      index = 0
      lines = make([]string, 10, 10)
    }
  }
}

func write(lines []string, client *fasthttp.Client) {
  if len(lines) < 1 {
    return
  }
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  var uri = host + "/" + index + "/_bulk"
  req.SetRequestURI(uri)
  var buf bytes.Buffer
  var meta = fmt.Sprintf(`{ "index" : {  } }%s`, "\n")
  for _, line := range lines {
    buf.WriteString(meta)
    buf.WriteString(line)
    buf.WriteString("\n")
  }
  req.SetBody(buf.Bytes())
  if err := client.Do(req, res); err != nil {
    fmt.Println("req error ", err)
    return
  }
  //result := string(res.Body())
  //fmt.Println(result)

}

func test() {
  client := util.GetFastHttpClient()
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  var uri = host + "/" + index + "/_bulk"
  req.SetRequestURI(uri)
  var buf bytes.Buffer
  var meta = fmt.Sprintf(`{ "index" : {  } }%s`, "\n")
  var item1 = "{\"@timestamp\": 893964617, \"clientip\":\"40.135.0.0\", \"request\": \"GET /images/hm_bg.jpg HTTP/1." + "0\", \"status\": 200, \"size\": 24736}"
  var item2 = "{\"@timestamp\": 893964653, \"clientip\":\"232.0.0.0\", \"request\": \"GET /images/hm_bg.jpg HTTP/1.0\", \"status\": 200, \"size\": 24736}"
  buf.WriteString(meta)
  buf.WriteString(item1)
  buf.WriteString("\n")
  buf.WriteString(meta)
  buf.WriteString(item2)
  buf.WriteString("\n")
  req.SetBody(buf.Bytes())
  if err := client.Do(req, res); err != nil {
    fmt.Println("req error ", err)
    return
  }
  result := string(res.Body())
  fmt.Println(result)
}
