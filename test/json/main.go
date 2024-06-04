package main

import (
  "bytes"
  "encoding/json"
  "es-demo/src/util"
  "fmt"
  "github.com/valyala/fasthttp"
  "os"
)

const HOST = "http://localhost:9200"

var path = "es-demo/doc/lst_mapping.json"
var setting = `{
  "index": {
	"number_of_shards": "2",
	"number_of_replicas": "1"
  }
}
`

func main() {
  content, _ := os.ReadFile(path)
  m := make(map[string]interface{})
  json.Unmarshal(content, &m)
  s := make(map[string]interface{})
  json.Unmarshal([]byte(setting), &s)
  client := util.GetFastHttpClient()
  create(client, "xxx", m, s)
}

func create(client *fasthttp.Client, index string, mapping, setting map[string]interface{}) {
  mappings := make(map[string]interface{}, 1)
  mappings[index] = mapping
  meta := make(map[string]interface{}, 2)
  meta["mappings"] = mappings
  meta["settings"] = setting
  body, _ := json.Marshal(meta)
  req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
  req.Header.SetMethod(fasthttp.MethodPut)
  req.Header.SetContentType("application/json")
  req.Header.Set("Authorization", "Basic ZWxhc3RpYzp3TDlPb2QyQA==")

  var buf bytes.Buffer
  buf.Write(body)
  var uri = HOST + "/" + index
  req.SetRequestURI(uri)
  req.SetBody(body)
  if err := client.Do(req, res); err != nil {
    fmt.Println("req error ", err)
    return
  }

}
