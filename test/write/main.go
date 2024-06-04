package main

import (
  "bufio"
  "bytes"
  "es-demo/src/util"
  "fmt"
  "github.com/valyala/fasthttp"
  "io"
  "os"
  "strings"
  "sync"
  "time"
)

var host = "http://localhost:9200"
var index = "test"
var mappingType = "_doc"

var batchCount = 100
var numberRoutines = 8

func main() {
  var path = "d://documents-241998.json"
  path = os.Args[1]
  //var path = "d://2.txt"
  inChan := make(chan string, 12000)
  exit := make(chan bool, 1)
  counterChan := make(chan int, 20)
  client := util.GetFastHttpClient()
  start := time.Now()
  go readFile(path, inChan)
  go writeEs(inChan, exit, client, counterChan)
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

func readFile(path string, inChan chan string) {
  file, err := os.Open(path)
  if err != nil {
    fmt.Println("open file error:", err)
    return
  }
  defer file.Close()
  reader := bufio.NewReaderSize(file, 4096*8)
  for {
    line, _, err := reader.ReadLine()
    if err == io.EOF {
      break
    }
    if len(line) < 1 {
      continue
    }
    inChan <- string(line)
  }
  close(inChan)
}

func writeEs(inChan chan string, exit chan bool, client *fasthttp.Client, counterChan chan int) {
  var wg sync.WaitGroup

  wg.Add(numberRoutines)
  for i := 0; i < numberRoutines; i++ {
    go doWriteES(inChan, &wg, client, counterChan)
  }
  wg.Wait()
  close(counterChan)
  exit <- true
  close(exit)
}

func doWriteES(inChan chan string, wg *sync.WaitGroup, client *fasthttp.Client, counterChan chan int) {
  var lines = make([]string, 0, batchCount)
  for {
    line, ok := <-inChan
    if !ok {
      if len(lines) > 0 {
        writeNew(lines, client)
        counterChan <- len(lines)
        lines = make([]string, 0, batchCount)
      }
      wg.Done()
      break
    }
    lines = append(lines, line)
    if len(lines) == cap(lines) {
      writeNew(lines, client)
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

func writeNew(lines []string, client *fasthttp.Client) {
  if len(lines) < 1 {
    return
  }
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPost)
  req.Header.SetContentType("application/json")
  var uri = host + "/_bulk"
  var s1, s2 string
  var buf bytes.Buffer
  for _, line := range lines {
    pair := strings.Split(line, "|||")
    if len(pair) != 2 {
      continue
    }
    if strings.Contains(pair[0], "_index") {
      s1 = pair[0]
      s2 = pair[1]
    } else {
      s1 = pair[1]
      s2 = pair[0]
    }
    buf.WriteString(s1 + "\n")
    buf.WriteString(s2 + "\n")

  }
  req.SetRequestURI(uri)
  req.SetBody(buf.Bytes())
  if err := client.Do(req, res); err != nil {
    fmt.Println("req error ", err)
    return
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
