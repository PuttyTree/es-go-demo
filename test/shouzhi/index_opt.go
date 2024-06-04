package main

import (
  "bytes"
  "encoding/json"
  "es-demo/src/util"
  "fmt"
  "github.com/samber/lo"
  "github.com/valyala/fasthttp"
  "math"
  "strings"
  "sync"
  "time"
)

const setting = `{
  "index": {
	"number_of_shards": "2",
	"number_of_replicas": "1"
  }
}
`

func test() {
  i := compareIndices(readIndicesByFile("D:\\1.txt"), readIndicesByFile("D:\\2.txt"))
  for _, i := range i {
    fmt.Println(i)
  }
}

// 批量创建索引
func createIndexBatch() {
  shards := getArg("-shards=")
  if len(shards) < 1 {
    fmt.Println("参数【-shards】缺失")
    return
  }
  loadConf()
  var auth = getAuth("writer")
  ep := strings.Split(Conf.Writer.Endpoint, ",")[0]
  var host = "http://" + ep
  s := make(map[string]interface{})
  json.Unmarshal([]byte(setting), &s)
  index := s["index"].(map[string]interface{})
  index["number_of_shards"] = shards
  client := util.GetFastHttpClient()

  start := time.Now()
  indices := readIndices()
  existed := catIndices(client, host, auth)
  left := compareIndices(indices, existed)
  var step = 50

  fmt.Printf("total indices=%v\n", len(left))
  counter := 0

  for i := 0; i <= len(left); i = i + step {
    end := math.Min(float64(i+step), float64(len(left)))
    sub := left[i:int(end)]
    var wg sync.WaitGroup
    wg.Add(len(sub))
    for j := 0; j < len(sub); j++ {
      m := make(map[string]interface{})
      json.Unmarshal([]byte(LST_MAPPING), &m)
      go create(client, host, sub[j], auth, m, s, &wg)
    }
    wg.Wait()
    time.Sleep(time.Second * 15)
    counter += len(sub)
    fmt.Printf("proccessed=%v\n", counter)
  }
  fmt.Printf("duration=%v", time.Now().Sub(start).Seconds())
}

func deleteIndices() {
  loadConf()
  indices := readIndices()
  if len(indices) < 1 {
    fmt.Println("indices empty")
    return
  }
  client := util.GetFastHttpClient()
  var auth = getAuth("writer")
  ep := strings.Split(Conf.Writer.Endpoint, ",")[0]
  var host = "http://" + ep
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(req)
  defer fasthttp.ReleaseResponse(res)
  req.Header.SetMethod(fasthttp.MethodDelete)
  req.Header.SetContentType("application/json")
  req.Header.Set("Authorization", auth)
  var step = 30
  var processed = 0
  for i := 0; i <= len(indices); i = i + step {
    end := math.Min(float64((i + step)), float64(len(indices)))
    sub := indices[i:int(end)]
    join := strings.Join(sub, ",")
    var uri = host + "/" + join
    req.SetRequestURI(uri)
    if err := client.Do(req, res); err != nil {
      fmt.Printf("req error,index=%v,host=%v,error=%v\n", join, host, err)
    }
    processed += len(sub)
    fmt.Printf("processed=%v,total=%v\n", processed, len(indices))
  }

}

func saveIndices() {
  loadConf()
  indices := readIndices()
  if len(indices) < 1 {
    fmt.Println("indices empty")
    return
  }
  for i := 1; i <= 1800; i++ {
    index1 := getIndex(i, "szjy_lst_dtl_bat")
    fmt.Println(index1)
  }

}

func catIndices(client *fasthttp.Client, host, auth string) []string {
  var url = host + "/_cat/indices?h=index"
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(req)
  defer fasthttp.ReleaseResponse(res)
  req.Header.SetMethod(fasthttp.MethodGet)
  req.Header.SetContentType("application/json")
  res.Header.Set("Accept", "application/json")
  if len(auth) > 0 {
    req.Header.Set("Authorization", auth)
  }
  req.SetRequestURI(url)
  if err := client.Do(req, res); err != nil {
    fmt.Println("req error ", err)
    return nil
  }
  return strings.Split(string(res.Body()), "\n")
}

func create(client *fasthttp.Client, host, index, auth string,
  mapping, setting map[string]interface{}, wg *sync.WaitGroup) {
  mappings := make(map[string]interface{}, 1)
  mappings[index] = mapping
  meta := make(map[string]interface{}, 2)
  meta["mappings"] = mappings
  meta["settings"] = setting
  body, _ := json.Marshal(meta)

  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer func() {
    fasthttp.ReleaseRequest(req)
    fasthttp.ReleaseResponse(res)
  }()
  req.Header.SetMethod(fasthttp.MethodPut)
  req.Header.SetContentType("application/json")
  req.Header.Set("Authorization", auth)

  var buf bytes.Buffer
  buf.Write(body)
  var uri = host + "/" + index + "?master_timeout=90s"
  req.SetRequestURI(uri)
  req.SetBody(body)
  if err := client.Do(req, res); err != nil {
    fmt.Printf("index=%v,req error:%v\n", index, err)
  }
  fmt.Printf("%v\n", string(res.Body()))
  if wg != nil {
    wg.Done()
  }

}

func compareIndices(totalIndices, partIndices []string) []string {
  if partIndices == nil {
    return totalIndices
  }
  var left []string
  for _, i := range totalIndices {
    flag := false

    for _, j := range partIndices {
      if i == j {
        flag = true
        break
      }
    }
    if flag {
      continue
    }
    left = append(left, i)
  }
  return left
}

func compareMapping() {
  loadConf()
  indices := readIndices()
  var authReader = getAuth("reader")
  epReader := strings.Split(Conf.Reader.Endpoint, ",")[0]
  var hostReader = "http://" + epReader
  var reqReader, resReader = fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(reqReader)
  defer fasthttp.ReleaseResponse(resReader)
  reqReader.Header.SetMethod(fasthttp.MethodGet)
  reqReader.Header.SetContentType("application/json")
  if len(authReader) > 0 {
    reqReader.Header.Set("Authorization", authReader)
  }

  var authWriter = getAuth("writer")
  epWriter := strings.Split(Conf.Writer.Endpoint, ",")[0]
  var hostWriter = "http://" + epWriter
  reqWriter, resWriter := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(reqWriter)
  defer fasthttp.ReleaseResponse(resWriter)
  reqWriter.Header.SetMethod(fasthttp.MethodGet)
  reqWriter.Header.SetContentType("application/json")
  if len(authWriter) > 0 {
    reqWriter.Header.Set("Authorization", authWriter)
  }

  client := util.GetFastHttpClient()
  for _, index := range indices {
    reqReader.SetRequestURI(hostReader + "/" + index + "/_mapping")
    reqWriter.SetRequestURI(hostWriter + "/" + index + "/_mapping")
    if err := client.Do(reqReader, resReader); err != nil {
      continue
    }
    if err := client.Do(reqWriter, resWriter); err != nil {
      continue
    }
    mapReader := make(map[string]interface{}, 1)
    mapWriter := make(map[string]interface{}, 1)
    json.Unmarshal(resReader.Body(), &mapReader)
    json.Unmarshal(resWriter.Body(), &mapWriter)
    doCompareMapping(mapReader, mapWriter, index)
  }
}

func doCompareMapping(mapReader, mapWriter map[string]interface{}, index string) {
  readerProperties := mapReader[index].(map[string]interface{})["mappings"].(map[string]interface{})[index].(map[string]interface{})["properties"].(map[string]interface{})
  writerProperties := mapWriter[index].(map[string]interface{})["mappings"].(map[string]interface{})[index].(map[string]interface{})["properties"].(map[string]interface{})
  for k, r := range readerProperties {
    rField := r.(map[string]interface{})
    rType := rField["type"]
    w := writerProperties[k]
    if w == nil {
      fmt.Printf("index=%v,readerField=%v,writerField=null\n", index, k)
      continue
    }
    wField := w.(map[string]interface{})
    wType := wField["type"]

    if rType != wType {
      fmt.Printf("index=%v,field=%v,readerType=%v,writerType=%v\n", index, k, rType, wType)
    }

  }

}

func compareCount() {
  loadConf()

  client := util.GetFastHttpClient()
  var authReader = getAuth("reader")
  epReader := strings.Split(Conf.Reader.Endpoint, ",")[0]
  var hostReader = "http://" + epReader
  var reqReader, resReader = fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(reqReader)
  defer fasthttp.ReleaseResponse(resReader)
  reqReader.Header.SetMethod(fasthttp.MethodGet)
  reqReader.Header.SetContentType("application/json")
  if len(authReader) > 0 {
    reqReader.Header.Set("Authorization", authReader)
  }
  reqReader.SetRequestURI(hostReader + "/_cat/indices?h=index,docs.count")
  if err := client.Do(reqReader, resReader); err != nil {
    fmt.Println("request reader error", err)
    return
  }

  var authWriter = getAuth("writer")
  epWriter := strings.Split(Conf.Writer.Endpoint, ",")[0]
  var hostWriter = "http://" + epWriter
  reqWriter, resWriter := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  defer fasthttp.ReleaseRequest(reqWriter)
  defer fasthttp.ReleaseResponse(resWriter)
  reqWriter.Header.SetMethod(fasthttp.MethodGet)
  reqWriter.Header.SetContentType("application/json")
  if len(authWriter) > 0 {
    reqWriter.Header.Set("Authorization", authWriter)
  }
  reqWriter.SetRequestURI(hostWriter + "/_cat/indices?h=index,docs.count")
  if err := client.Do(reqWriter, resWriter); err != nil {
    fmt.Println("request writer error", err)
    return
  }
  readerCounter := strings.Split(string(resReader.Body()), "\n")
  writerCounter := strings.Split(string(resWriter.Body()), "\n")
  indices := readIndices()
  for _, index := range indices {
    r, _ := lo.Find(readerCounter, func(item string) bool {
      return strings.HasPrefix(item, index)
    })
    w, _ := lo.Find(writerCounter, func(item string) bool {
      return strings.HasPrefix(item, index)
    })
    if len(r) > 0 && len(w) > 0 {
      rr := strings.Split(r, " ")
      ww := strings.Split(w, " ")
      if rr[1] != ww[1] {
        fmt.Printf("index=%v,reader=%v,writer=%v\n", index, rr[1], ww[1])
      }
    }
  }
}

/*
func outputIndex(path string) {
	var file *os.File
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}
	file, _ = os.Create(path)
	defer file.Close()

	w := bufio.NewWriter(file)
	writer := bufio.NewWriterSize(w, 4096)
	for i := 1; i <= 1800; i++ {
		s := getIndex(i, TEMPLATE)
		writer.WriteString(s + "\n")
	}

	writer.Flush()
}
*/
