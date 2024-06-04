package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"es-demo/src/entity"
	"es-demo/src/util"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// mock 数据到文件
// -o=必填
// -t=参数可选
func startMockToFile() {
	jsonPath := getArg("-o=")
	if len(jsonPath) < 1 {
		fmt.Println("no parameter -o for output file path")
		return
	}
	fmt.Printf("output=%v\n", jsonPath)
	loadConf()
	indices := readIndices()
	inChan := make(chan []entity.SzjyLstDtlBat, 500000)
	counterChan := make(chan int, 20)
	ctx, cancel := context.WithCancel(context.Background())
	go produce(inChan, indices)
	go consumeToFile(inChan, jsonPath, cancel, counterChan)
	go count(counterChan, 0)
	finish(ctx)
}

// mock数据到es
func startMockToEs() {
	indices := readIndices()
	loadConf()
	inChan := make(chan []entity.SzjyLstDtlBat, 500000)
	counterChan := make(chan int, 20)
	ctx, cancel := context.WithCancel(context.Background())
	go produce(inChan, indices)
	go consumeToEs(inChan, cancel, counterChan)
	go count(counterChan, 0)
	finish(ctx)
}

func readFileToEs() {
	jsonPath := getArg("-i=")
	if len(jsonPath) < 1 {
		fmt.Println("no parameter -i for output file path")
		return
	}
	inChan := make(chan string, 12000)
	exit := make(chan bool, 1)
	counterChan := make(chan int, 20)
	client := util.GetFastHttpClient()
	start := time.Now()
	go readFile(jsonPath, inChan)
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

func readFile(path string, inChan chan string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("open file error:", err)
		return
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 4096*1400)
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

	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go doWriteES(inChan, &wg, client, counterChan)
	}
	wg.Wait()
	close(counterChan)
	exit <- true
	close(exit)
}

func doWriteES(inChan chan string, wg *sync.WaitGroup, client *fasthttp.Client, counterChan chan int) {
	var lines = make([]string, 0, BULK_SIZE)
	auth := getAuth("reader")
	for {
		line, ok := <-inChan
		if !ok {
			if len(lines) > 0 {
				writeNew(lines, client, auth)
				counterChan <- len(lines)
				lines = make([]string, 0, BULK_SIZE)
			}
			wg.Done()
			break
		}
		lines = append(lines, line)
		if len(lines) == cap(lines) {
			writeNew(lines, client, auth)
			counterChan <- len(lines)
			lines = make([]string, 0, BULK_SIZE)
		}
	}

}

func writeNew(lines []string, client *fasthttp.Client, auth string) {
	if len(lines) < 1 {
		return
	}
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	if len(auth) > 0 {
		req.Header.Set("Authorization", auth)
	}
	var uri = "http://" + strings.Split(Conf.Reader.Endpoint, ",")[0] + "/_bulk"

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

func produce(inChan chan []entity.SzjyLstDtlBat, indices []string) {
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go doProduce(inChan, &wg, indices)
	}
	wg.Wait()
	close(inChan)
}

func consumeToEs(inChan chan []entity.SzjyLstDtlBat, cancel context.CancelFunc,
	counterChan chan int) {
	client := util.GetFastHttpClient()

	var input [][]entity.SzjyLstDtlBat
	auth := getAuth("reader")
	for {
		arr, ok := <-inChan
		if !ok {
			break
		}
		if len(input) == numRoutines {
			var wg sync.WaitGroup
			wg.Add(len(input))
			for i := 0; i < len(input); i++ {
				go writeToEs(client, auth, input[i], &wg)
			}
			wg.Wait()
			input = nil
		} else {
			input = append(input, arr)
			counterChan <- len(arr)
		}
	}
	close(counterChan)
	cancel()
}

func consumeToFile(inChan chan []entity.SzjyLstDtlBat, jsonPath string, cancel context.CancelFunc, counterChan chan int) {
	file, _ := os.OpenFile(jsonPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	w := bufio.NewWriter(file)
	writer := bufio.NewWriterSize(w, 4096*1400)
	var input [][]entity.SzjyLstDtlBat
	for {
		arr, ok := <-inChan
		if !ok {
			break
		}
		if len(input) == numRoutines {
			var wg sync.WaitGroup
			wg.Add(len(input))
			for i := 0; i < len(input); i++ {
				tmpI := i
				go writeToFile(writer, input[tmpI], &wg, counterChan)
			}
			wg.Wait()
			writer.Flush()
			input = nil
		} else {
			input = append(input, arr)
		}
	}
	writer.Flush()
	close(counterChan)
	cancel()
}

func writeToFile(writer *bufio.Writer, szjyLstDtlBats []entity.SzjyLstDtlBat, wg *sync.WaitGroup, counterChan chan int) {
	var meta string
	for _, bat := range szjyLstDtlBats {
		id := randStr(24) + "@@@" + bat.EacId
		meta = fmt.Sprintf(`{ "index" : { "_index":"%s","_type":"%s","_id":"%s","_routing":"%s" } }`,
			bat.Index, bat.Index, id, bat.EacId)
		source, _ := json.Marshal(bat)
		s := meta + "|||" + string(source) + "\n"
		writer.WriteString(s)
	}
	counterChan <- len(szjyLstDtlBats)
	if wg != nil {
		wg.Done()
	}
}

func writeToEs(client *fasthttp.Client, auth string, szjyLstDtlBats []entity.SzjyLstDtlBat,
	wg *sync.WaitGroup) {
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	if len(auth) > 0 {
		req.Header.Set("Authorization", auth)
	}
	//var uri = "http://" + strings.Split(Conf.Reader.Endpoint, ",")[0] + "/" + tarIndex + "/" + tarIndex + "/_bulk"
	var uri = "http://" + strings.Split(Conf.Reader.Endpoint, ",")[0] + "/_bulk"
	req.SetRequestURI(uri)
	var buf bytes.Buffer
	var meta string
	for _, bat := range szjyLstDtlBats {
		id := randStr(24) + "@@@" + bat.EacId
		meta = fmt.Sprintf(`{ "index" : {  "_index":"%s","_type":"%s","_id":"%s","_routing":"%s" } } %s`,
			bat.Index, bat.Index, id, bat.EacId, "\n")
		buf.WriteString(meta)
		source, _ := json.Marshal(bat)
		buf.WriteString(string(source))
		buf.WriteString("\n")
	}
	req.SetBody(buf.Bytes())
	if err := client.Do(req, res); err != nil {
		fmt.Println("req error ", err)
	}
	if wg != nil {
		wg.Done()
	}

}

func doProduce(inChan chan []entity.SzjyLstDtlBat, wg *sync.WaitGroup, indices []string) {
	for i := 0; i < LOOP; i++ {
		var arr []entity.SzjyLstDtlBat
		index := indices[i%len(indices)]
		for i := 0; i < BULK_SIZE; i++ {
			arr = append(arr, oneSzjyLstDtlBat(index))
		}
		inChan <- arr
	}

	wg.Done()
}

func getIndexSeq() int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(1800)
}

func getIndex(seq int, tarIndex string) string {
	month := "0" + strconv.Itoa(seq%9)
	if month == "00" {
		month = "09"
	}
	suffix := ""
	if seq < 10 {
		suffix = "000" + strconv.Itoa(seq)
	} else if seq < 100 {
		suffix = "00" + strconv.Itoa(seq)
	} else if seq < 1000 {
		suffix = "0" + strconv.Itoa(seq)
	} else {
		suffix = strconv.Itoa(seq)
	}
	index := tarIndex + "_" + Conf.Year + month + "_" + suffix
	return index
}

func getEacId(x int) string {
	n := int64(x)
	prefix := randDigital(560000, 5555554)*1800 + n
	suffix := randDigital(10000, 99999)
	s := strconv.FormatInt(prefix, 10) + "EAP" + strconv.FormatInt(suffix, 10)
	return s
}

func finish(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("finish goroutine by context!")
			return
		default:
			time.Sleep(time.Second)
		}

	}

}

func oneSzjyLstDtlBat(index string) entity.SzjyLstDtlBat {
	arr := strings.Split(index, "_")
	seq, _ := strconv.Atoi(arr[len(arr)-1])
	return entity.SzjyLstDtlBat{
		Index:       index,
		BalAmt:      rand.Float32() * 100,
		CcyCod:      strconv.Itoa(rand.Intn(100)),
		ChnCod:      strconv.Itoa(rand.Intn(100)),
		ChnNam:      "旧动优势",
		DatDte:      randTime("2006-01-02"),
		DatFlg1:     getAOrB(),
		DatFlg2:     getYOrN(),
		DatFlg3:     getDatFlg3(),
		DatMon:      randTime("200601"),
		Db1Cod:      getDb1Cod(),
		DtlTyp:      "01",
		EacId:       getEacId(seq),
		F1:          randTime("2006-01-02"),
		F2:          "J3TI",
		F3:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F4:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F5:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F6:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F7:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F8:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F9:          "aaaaaaaaaaaaaaaaaaaaaaa",
		F10:         "aaaaaaaaaaaaaaaaaaaaaaa",
		FileFlag:    "aaaaaaaaaaaaaaaaaaaaaaa",
		FuzzySearch: "旧动优势@@@交易备注79@@@本人@@@收入@@@投资收益@@@@@@@@@@@@信用卡@@@@@@@@@@@@退款@@@美元@@@",
		MchCod:      "429598",
		Mch2Cod:     "084617317438173",
		PayEac:      "6225792409969776",
		Picture:     getYOrN(),
		RcvEac:      "6225316435272361",
		RmbAmt:      rand.Float64() * 10000,
		TrxAmt:      rand.Float64() * 10000,
		TrxCod1:     rand.Int31n(100),
		TrxCod2:     rand.Int31n(100),
		TrxCod3:     getAOrB(),
		TrxCrdNbr:   "LU76shouzhi001X5X0ND7SMA@@@自动化造账户号XXXX626667",
		TrxDte:      randTime("2006-01-02"),
		TrxTim:      randTime("2006-01-02 15:04:05"),
	}
}

func getDb1Cod() string {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(140)
	if n < 10 {
		return "00" + strconv.Itoa(n)
	} else if n < 100 {
		return "0" + strconv.Itoa(n)
	} else {
		return strconv.Itoa(n)
	}

}
func getAOrB() string {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(100)
	if n%2 == 0 {
		return "A"
	} else {
		return "B"
	}
}

func getYOrN() string {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(100)
	if n%2 == 0 {
		return "Y"
	} else {
		return "N"
	}
}

func getDatFlg3() string {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(100) % 4
	if n == 0 {
		return "A"
	} else if n == 1 {
		return "B"
	} else if n == 2 {
		return "C"
	} else {
		return "D"
	}
}
func randTime(format string) string {
	rand.Seed(time.Now().Unix())
	year, _ := strconv.Atoi(Conf.Year)
	min := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(year, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
	randomTime := time.Unix(rand.Int63n(max-min)+min, 0).Format(format) //
	return randomTime
}

func randDigital(min, max int64) int64 {
	rand.Seed(time.Now().Unix())
	return rand.Int63n(max-min) + min
}

func randStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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
