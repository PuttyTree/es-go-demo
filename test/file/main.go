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
  env      = "env=st"
  local_ip = "local_ip="

  source_ip = "source_ip="
  //#数据源端口
  source_port = "source_port=9200"
  source_name = "source_name="

  //#目标存储ip
  target_ip = "target_ip="
  //#目标存储端口
  target_port = "target_port=9200"
  //#目标存储名（如是es的话就是集群名）
  target_name = "target_name="

  //#一批提交的数量
  batch_size = "batch_size=100"
  //#针对文件的线程数
  thread_num = "thread_num=10"
  //#单个文件线程数
  one_file_thread_num = ""

  esrp_host = ""
  users     = ""
)
const year int = 2020
const disk = "D:\\"
const root_dir = "xxx"
const template_file = "es-demo/doc/transform.properties"
const step = 50

var parent_dir = disk + root_dir + "\\" + strconv.Itoa(year)
var index_file = parent_dir + "\\indices.txt"

func main() {
  indices := readIndices(index_file)
  templates := readTemplateFile(template_file)
  loop(parent_dir, indices, templates)
}

func readIndices(path string) []string {
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
  return indices
}

func loop(path string, indices, templates []string) {
  for i := 0; i < len(indices); i = i + step {
    end := math.Min(float64(i+step), float64(len(indices)))
    sub := indices[i:int(end)]
    sourceIndices := strings.Join(sub, ",")
    writeConfig(path, strconv.Itoa(i), sourceIndices, templates)
  }
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

func writeConfig(parentDir string, name string, sourceIndices string, templates []string) {
  var dir = parentDir + string(os.PathSeparator) + name
  _, err := os.Stat(dir)
  var prefix = "java -cp fast-dump"
  var properties = root_dir + "/" + strconv.Itoa(year) + "/" + name + "/transform.properties"
  var log = root_dir + "/" + strconv.Itoa(year) + "/" + name + "/nohup.log"
  var suffix = " 2>&1"
  fmt.Println(prefix + properties + ">" + log + suffix)
  if os.IsNotExist(err) {
    os.Mkdir(dir, os.ModePerm)
  }

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
  writer.Flush()
  writer.WriteString(env + new_line)
  writer.WriteString(local_ip + new_line)
  writer.WriteString(source_ip + new_line)
  writer.WriteString(source_port + new_line)
  writer.WriteString(source_name + new_line)
  writer.WriteString(target_ip + new_line)
  writer.WriteString(target_port + new_line)
  writer.WriteString(target_name + new_line)
  writer.WriteString(batch_size + new_line)
  writer.WriteString(thread_num + new_line)
  writer.WriteString(one_file_thread_num + new_line)
  writer.WriteString(esrp_host + new_line)
  writer.WriteString(users + new_line)
  writer.WriteString("source_index=" + sourceIndices + "\n")
  writer.Flush()
}
