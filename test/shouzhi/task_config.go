package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

const (
	new_line = "\n"
	//env      = "env=st"
	//跳板机ip
	//local_ip = "local_ip=55.122.16.34"

	//source_ip = "source_ip=55.13.64.91,55.13.63.92"
	//#数据源端口
	//source_port     = "source_port=9200"
	//source_name     = "source_name=es-test1-vop003"
	//source_username = "source_username="
	//source_password = "source_password="

	//#目标存储ip
	//target_ip = "target_ip=55.13.63.95,55.13.64.95"
	//#目标存储端口
	//target_port = "target_port=9200"
	//#目标存储名（如是es的话就是集群名）
	//target_name     = "target_name=es-test1-vop002"
	//target_username = "target_username=elastic"
	//target_password = "target_password=wL9Ood2@"

	//#一批提交的数量
	//batch_size = "batch_size=1000"
	//#批量读取文件的线程数
	//thread_num = "thread_num=4"
	//#单个文件切分线程数
	//one_file_thread_num = "one_file_thread_num=2"

	//esrp_host    = "esrp_host=http://elasticsearchrp.paasuat.cmbchina.cn"
	//zhaohu_users = "zhaohu_users=80318002"
)

//const step = 500

// 生成fastdump迁移任务
func generateTask() {
	loadConf()
	indices := readIndices()
	if len(indices) < 1 {
		fmt.Println("indices empty")
		return
	}
	dumpTasks := getArg("-dumpTasks=")
	if len(dumpTasks) < 1 {
		fmt.Println("no parameter for -dumpTasks")
		return
	}

	templateFile := strings.Join([]string{Conf.Disk, Conf.ParentDir, "transform.properties"}, string(os.PathSeparator))
	templates := readTemplateFile(templateFile)
	taskDir := strings.Join([]string{Conf.Disk, Conf.ParentDir, Conf.Year, Conf.Reader.Cluster},
		string(os.PathSeparator))
	step, _ := strconv.Atoi(dumpTasks)
	doGenerate(taskDir, indices, templates, step)
}

func doGenerate(path string, indices, templates []string, step int) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.MkdirAll(path, os.ModePerm)
	}

	var shellPath = path + string(os.PathSeparator) + "run.sh"
	_, err = os.Stat(shellPath)
	if err == nil {
		os.Remove(path)
	}
	shellFile, err := os.Create(shellPath)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}

	}(shellFile)
	w := bufio.NewWriter(shellFile)
	shellWriter := bufio.NewWriterSize(w, 4096)
	shellWriter.WriteString("#! /bin/bash" + new_line)
	shellWriter.WriteString(" " + new_line)
	for i := 0; i < len(indices); i = i + step {
		end := math.Min(float64(i+step), float64(len(indices)))
		sub := indices[i:int(end)]
		sourceIndices := strings.Join(sub, ",")
		writeConfig(path, strconv.Itoa(i), sourceIndices, templates, shellWriter)
	}
	shellWriter.Flush()
}

func writeConfig(parentDir string, name string, sourceIndices string, templates []string, shellWriter *bufio.Writer) {
	var dir = parentDir + string(os.PathSeparator) + name
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	var prefix = "java -cp fast-dump-1.0-allinone.jar com.youzan.clouddb.tools.general.RecordScannerMaster "
	var parent = Conf.ParentDir + "/" + Conf.Year + "/" + Conf.Reader.Cluster

	var properties = parent + "/" + name + "/transform.properties"
	var log = parent + "/" + name + "/nohup.log"
	var suffix = " 2>&1"
	var cmd = prefix + properties + ">" + log + suffix
	shellWriter.WriteString(cmd + new_line)
	fmt.Println(cmd)

	var path = dir + string(os.PathSeparator) + "transform.properties"
	_, err = os.Stat(path)
	if err == nil {
		os.Remove(path)
	}
	file, err := os.Create(path)

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}

	}(file)

	w := bufio.NewWriter(file)
	writer := bufio.NewWriterSize(w, 4096)
	for _, line := range templates {
		writer.WriteString(line + new_line)
	}
	var ips []string
	var port string
	writer.Flush()
	writer.WriteString("env=" + Conf.Env + new_line)
	writer.WriteString("local_ip=" + Conf.LocalIp + new_line)
	eps := strings.Split(Conf.Reader.Endpoint, ",")
	for _, v := range eps {
		ips = append(ips, strings.Split(v, ":")[0])
		port = strings.Split(v, ":")[1]
	}
	writer.WriteString("source_ip=" + strings.Join(ips, ",") + new_line)
	writer.WriteString("source_port=" + port + new_line)
	writer.WriteString("source_name=" + Conf.Reader.Cluster + new_line)
	writer.WriteString("source_username=" + Conf.Reader.AccessId + new_line)
	writer.WriteString("source_password=" + Conf.Reader.AccessKey + new_line)
	ips = nil
	eps = strings.Split(Conf.Writer.Endpoint, ",")
	for _, v := range eps {
		ips = append(ips, strings.Split(v, ":")[0])
		port = strings.Split(v, ":")[1]
	}
	writer.WriteString("target_ip=" + strings.Join(ips, ",") + new_line)
	writer.WriteString("target_port=" + port + new_line)
	writer.WriteString("target_name=" + Conf.Writer.Cluster + new_line)
	writer.WriteString("target_username=" + Conf.Writer.AccessId + new_line)
	writer.WriteString("target_password=" + Conf.Writer.AccessKey + new_line)
	writer.WriteString("batch_size=" + strconv.Itoa(Conf.BatchSize) + new_line)
	writer.WriteString("thread_num=" + strconv.Itoa(Conf.ThreadNum) + new_line)
	writer.WriteString("one_file_thread_num=" + strconv.Itoa(Conf.OneFileThreadNum) + new_line)
	writer.WriteString("esrp_host=" + Conf.EsrpHost + new_line)
	writer.WriteString("zhaohu_users=" + Conf.ZhaohuUsers + new_line)
	writer.WriteString("source_index=" + sourceIndices + "\n")
	writer.Flush()

}

func readTemplateFile(path string) []string {
	file, _ := os.Open(path)
	defer file.Close()
	reader := bufio.NewReaderSize(file, 4096*8)
	var lines []string
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		lines = append(lines, string(line))
	}
	return lines

}
