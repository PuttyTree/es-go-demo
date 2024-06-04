package main

import (
	"bufio"
	"encoding/json"
	"es-demo/src/entity"
	"es-demo/src/util"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"os"
	"strings"
)

const URI = "/datax/console/job/es/add"

func addDataxTasks() {
	conf := loadConf()
	indices := readIndices()
	dataxFile := getArg("-d=")
	if len(dataxFile) < 1 {
		fmt.Println("no parameter for -d")
		return
	}
	fmt.Printf("dataxFile=%v\n", dataxFile)
	dataxConf := readDataxConf(dataxFile)
	auth := dataxConf["auth"].(string)
	client := util.GetFastHttpClient()
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.Header.Set("Accept-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("UserId", "80318002@itc")
	req.Header.Set("X-SSO-USER", "80318002")
	req.Header.Set("X-ARIUS-APP-TICKET", "xTc59aY72")
	req.Header.Set("X-B3-BusinessId", "LD2301ACSESUI")
	if len(auth) > 0 {
		req.Header.Set("Authorization", auth)
	}
	fmt.Printf("total indices=%v\n", len(indices))
	var url = dataxConf["host"].(string) + URI
	req.SetRequestURI(url)
	counter := 0
	for _, index := range indices {
		parameter := buildParameter(conf, index, dataxConf)
		marshal, _ := json.Marshal(parameter)
		req.SetBody(marshal)
		if err := client.Do(req, res); err != nil {
			fmt.Println("req error ", err)
		} else {
			m := make(map[string]interface{}, 1)
			json.Unmarshal(res.Body(), &m)
			code := int(m["code"].(float64))
			if code != 1 {
				fmt.Printf("index=%v,error=%v\n", index, m["msg"])
			}
		}
		counter++
	}
	fmt.Printf("finished indices=%v\n", counter)
}

func readDataxConf(path string) map[string]interface{} {
	file, _ := os.Open(path)
	defer file.Close()
	reader := bufio.NewReaderSize(file, 4096)
	m := make(map[string]interface{}, 8)
	for {
		line, _, err := reader.ReadLine()
		if len(line) < 1 || err == io.EOF {
			break
		}
		split := strings.Split(strings.TrimSpace(string(line)), "=")
		m[split[0]] = split[1]
	}
	return m
}
func buildParameter(conf *entity.Config, index string, dataxConf map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	json.Unmarshal([]byte(TASK_TEMPLATE), &m)
	m["targetInstance"] = dataxConf["targetInstance"]
	m["projectId"] = dataxConf["targetId"]
	m["targetCluster"] = conf.Writer.Cluster

	content := m["content"].([]interface{})[0].(map[string]interface{})
	readerParam := content["reader"].(map[string]interface{})["parameter"].(map[string]interface{})
	writerParam := content["writer"].(map[string]interface{})["parameter"].(map[string]interface{})
	readerParam["endpoint"] = conf.Reader.Endpoint
	readerParam["accessId"] = conf.Reader.AccessId
	readerParam["accessKey"] = conf.Reader.AccessKey
	readerParam["index"] = index
	readerParam["type"] = index

	writerParam["endpoint"] = conf.Writer.Endpoint
	writerParam["accessId"] = conf.Writer.AccessId
	writerParam["accessKey"] = conf.Writer.AccessKey
	writerParam["index"] = index
	writerParam["type"] = index
	return m
}
