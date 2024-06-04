package util

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"time"
)

func GetFastHttpClient() *fasthttp.Client {
	reqClient := &fasthttp.Client{
		// 读超时时间,不设置read超时,可能会造成连接复用失效
		ReadTimeout: time.Second * 90,
		// 写超时时间
		WriteTimeout: time.Second * 90,
		// 60秒后，关闭空闲的活动连接
		MaxIdleConnDuration: time.Second * 60,
		// 当true时,从请求中去掉User-Agent标头
		NoDefaultUserAgentHeader: true,
		// 当true时，header中的key按照原样传输，默认会根据标准化转化
		DisableHeaderNamesNormalizing: true,
		//当true时,路径按原样传输，默认会根据标准化转化
		DisablePathNormalizing: true,
		Dial: (&fasthttp.TCPDialer{
			// 最大并发数，0表示无限制
			Concurrency: 4096,
			// 将 DNS 缓存时间从默认分钟增加到一小时
			DNSCacheDuration: time.Hour,
		}).Dial,
	}
	return reqClient
}

/**
 * 发起Get请求
**/
func Get() string {
	// 获取客户端
	client := GetFastHttpClient()
	// 从请求池中分别获取一个request、response实例
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	// 回收实例到请求池
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()
	// 设置请求方式
	req.Header.SetMethod(fasthttp.MethodGet)
	// 设置请求地址
	req.SetRequestURI("http://127.0.0.1/test")
	// 设置参数
	var arg fasthttp.Args
	arg.Add("name", "张三")
	arg.Add("id", "10001")
	req.URI().SetQueryString(arg.String())
	// 设置header信息
	//req.Header.Add(HAppIdKey, HAppIdVal)
	// 设置Cookie信息
	req.Header.SetCookie("key", "val")
	// 发起请求
	if err := client.Do(req, resp); err != nil {
		fmt.Println("req err ", err)
		return err.Error()
	}
	// 读取结果
	return string(resp.Body())
}

// post请求参数
type postParamExample struct {
	Id   int    `json:"id,omitempty"`
	Name string `json:"app_id_list,omitempty"`
}

/**
 * post请求
**/
func Post() string {
	// 获取客户端
	client := GetFastHttpClient()
	// 从请求池中分别获取一个request、response实例
	req, res := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	// 回收到请求池
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(res)
	}()
	// 设置请求方式
	req.Header.SetMethod(fasthttp.MethodPost)
	// 设置请求地址
	req.SetRequestURI("http://127.0.0.1/test")
	// 设置请求ContentType
	req.Header.SetContentType("application/json")
	// 设置参数
	param := postParamExample{
		Id:   10001,
		Name: "小明",
	}
	marshal, _ := json.Marshal(param)
	req.SetBodyRaw(marshal)
	// 发起请求
	if err := client.Do(req, res); err != nil {
		fmt.Println("req err ", err)
		return err.Error()
	}
	// 读取结果
	return string(res.Body())
}
