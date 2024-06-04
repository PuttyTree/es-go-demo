package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"es-demo/src/entity"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// 创建动态迁移的conf.json和indices.txt文件
func configDynamicClustersMeta(csvPath string) {
	if len(csvPath) < 1 {
		fmt.Println("cluster.csv parameter empty")
		return
	}
	conf := loadConf()
	file, _ := os.Open(csvPath)
	defer file.Close()

	reader := csv.NewReader(file)
	clusters, _ := reader.ReadAll()
	dir := filepath.Dir(csvPath)
	for i, cluster := range clusters {
		if i == 0 {
			continue
		}
		split := strings.Split(cluster[0], "/")
		for _, month := range split {
			num, err := strconv.Atoi(month)
			if err != nil {
				continue
			}
			year := strconv.Itoa(num)[0:4]
			path := dir + string(os.PathSeparator) + year + ".csv"
			loadIndices(path, year, month, cluster[2], cluster[3], cluster[4], conf)
		}
	}
}

// 创建静态迁移的conf.json和indices.txt文件
func configStaticClustersMeta(csvPath string) {
	if len(csvPath) < 1 {
		fmt.Println("csvPath empty")
		return
	}
	conf := loadConf()
	file, _ := os.Open(csvPath)
	defer file.Close()

	reader := csv.NewReader(file)
	clusters, _ := reader.ReadAll()
	dir := filepath.Dir(csvPath)
	allIndices := getStaticFullIndices(dir)
	for i, v := range clusters {
		if i == 0 {
			continue
		}
		split := strings.Split(v[0], "/")
		cluster := v[1]
		for _, s := range split {
			indices := getStaticIndices(allIndices, s)
			if indices == nil {
				continue
			}
			year := s
			num, err := strconv.Atoi(s)
			if err == nil {
				year = strconv.Itoa(num)[0:4]
			}
			config := writeConf(year, cluster, v[2], v[3], v[4], CONF2, conf)
			writeIndices(config, indices, INDICES2)

		}
	}
}

func getStaticFullIndices(dir string) [][]string {
	path := dir + string(os.PathSeparator) + "full.csv"
	file, _ := os.Open(path)
	defer file.Close()
	reader := csv.NewReader(file)
	indices, _ := reader.ReadAll()
	return indices
}

func getStaticIndices(allIndices [][]string, key string) []string {
	var temp [][]string
	for _, v := range allIndices {
		if v[0] == key {
			temp = append(temp, v)
		}
	}
	if len(temp) < 1 {
		return nil
	}
	var indices []string
	for _, v := range temp {
		indices = append(indices, v[4])
	}
	return indices
}

func loadIndices(path string, year, month, endpoint, account, pwd string, conf *entity.Config) {
	_, err := os.Stat(path)
	if err != nil {
		return
	}
	file, _ := os.Open(path)
	defer file.Close()

	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	var cluster string
	var indices []string
	var flag = false
	for i, v := range records {
		if i == 0 {
			continue
		}
		if flag && len(v[0]) > 0 && v[0] != month {
			break
		}
		if len(v[0]) > 0 && v[0] == month {
			cluster = strings.TrimSpace(v[1])
			flag = true
		}
		if flag {
			indices = append(indices, strings.TrimSpace(v[4]))
		}
	}
	if len(indices) > 0 {
		config := writeConf(year, cluster, endpoint, account, pwd, CONF1, conf)
		writeIndices(config, indices, INDICES1)
	}
}

func writeConf(year string, cluster, endpoint, account, pwd string, confName string, conf *entity.Config) *entity.Config {
	clone, _ := DeepClone(conf)
	var confClone = clone.(*entity.Config)
	confClone.Year = year
	confClone.Reader.Cluster = cluster
	confClone.Reader.Endpoint = endpoint
	confClone.Reader.AccessId = account
	confClone.Reader.AccessKey = pwd

	path := strings.Join([]string{confClone.Disk, confClone.ParentDir, confClone.Year, cluster, confName},
		string(os.PathSeparator))
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}
	dir := filepath.Dir(path)
	_, err = os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, os.ModePerm)
	}
	confFile, err := os.Create(path)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}

	}(confFile)
	bytes, _ := json.Marshal(confClone)
	confFile.Write(bytes)
	return confClone
}

func writeIndices(conf *entity.Config, indices []string, indicesName string) {
	fmt.Println(indices)
	path := strings.Join([]string{conf.Disk, conf.ParentDir, conf.Year, conf.Reader.Cluster, indicesName},
		string(os.PathSeparator))
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}
	indexFile, err := os.Create(path)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}

	}(indexFile)
	w := bufio.NewWriter(indexFile)
	writer := bufio.NewWriterSize(w, 4096)
	for _, i := range indices {
		writer.WriteString(i + new_line)
	}
	writer.Flush()
}
