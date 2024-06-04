package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"es-demo/src/entity"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
)

const numRoutines = 16
const LOOP = 3600 * 4
const BULK_SIZE = 300

const CONF1 = "conf1.json"
const CONF2 = "conf2.json"
const INDICES1 = "indices1.txt"
const INDICES2 = "indices2.txt"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var Conf *entity.Config = nil

/*
*
-m=method
-c=conf.json
-t=indices.txt
-i=inputData.json
-o=outputData.json
-v=input.csv
-d=datax.txt
*/
func main() {
	switch method := getArg("-m="); method {
	case "compareIndices":
		test()
	case "addDataxTasks":
		addDataxTasks()
	case "createIndexBatch":
		createIndexBatch()
	case "deleteIndices":
		deleteIndices()
	case "saveIndices":
		saveIndices()
	case "generateTask":
		generateTask()
	case "configDynamicClustersMeta":
		configDynamicClustersMeta(getArg("-v="))
	case "configStaticClustersMeta":
		configStaticClustersMeta(getArg("-v="))
	case "startMockToFile":
		startMockToFile()
	case "startMockToEs":
		startMockToEs()
	case "readFileToEs":
		readFileToEs()
	case "compareCount":
		compareCount()
	case "compareMapping":
		compareMapping()
	case "transport":
		transport()
	default:
		fmt.Println("no method parameter")
	}
}

func readIndices() []string {
	indexFile := getArg("-t=")
	if len(indexFile) < 1 && Conf != nil {
		indexFile = strings.Join([]string{Conf.Disk, Conf.ParentDir, Conf.Year, Conf.Reader.Cluster,
			INDICES1}, string(os.PathSeparator))
	}
	fmt.Printf("indexFile=%v\n", indexFile)
	file, _ := os.Open(indexFile)
	defer file.Close()
	reader := bufio.NewReaderSize(file, 4096)
	var indices []string
	for {
		line, _, err := reader.ReadLine()
		if len(line) < 1 || err == io.EOF {
			break
		}
		indices = append(indices, strings.TrimSpace(string(line)))
	}
	return indices
}

func readIndicesByFile(indexFile string) []string {
	fmt.Printf("indexFile=%v\n", indexFile)
	file, _ := os.Open(indexFile)
	defer file.Close()
	reader := bufio.NewReaderSize(file, 4096)
	var indices []string
	for {
		line, _, err := reader.ReadLine()
		if len(line) < 1 || err == io.EOF {
			break
		}
		indices = append(indices, strings.TrimSpace(string(line)))
	}
	return indices
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

// 深度克隆，可以克隆任意数据类型
func DeepClone(src interface{}) (interface{}, error) {
	typ := reflect.TypeOf(src)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		dst := reflect.New(typ).Elem()
		b, _ := json.Marshal(src)
		if err := json.Unmarshal(b, dst.Addr().Interface()); err != nil {
			return nil, err
		}
		return dst.Addr().Interface(), nil
	} else {
		dst := reflect.New(typ).Elem()
		b, _ := json.Marshal(src)
		if err := json.Unmarshal(b, dst.Addr().Interface()); err != nil {
			return nil, err
		}
		return dst.Interface(), nil
	}
}
