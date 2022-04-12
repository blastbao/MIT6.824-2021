package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	for {
		// 发送心跳给 Coordinator
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		// 获取任务信息
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {

	// 文件路径
	fileName := response.FilePath

	// 打开文件
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	// 读取内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	// 执行 Map 得到 kvs
	kva := mapF(fileName, string(content))

	// 按 Reduce 数目分组，将 kvs 按 hash(key) 拆分成 []kvs
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := iHash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}

	// 将 []kvs 写入到中间文件，作为后续 Reduce 任务的输入
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			// file := "mr-mapId-reduceId"
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			// 写到中间文件
			atomicWriteFile(intermediateFilePath, &buf)
		}(index, intermediate)
	}
	wg.Wait()

	// 上报 map(id) 任务完成
	doReport(response.Id, MapPhase)
}

func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	// 汇总每个 map 生成的、用于输入到 reducer(i) 的中间文件数据
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		// 读取中间文件
		filePath := generateMapResultFileName(i, response.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	results := make(map[string][]string)

	// Maybe we need merge sort for larger data
	// 按 key 汇总
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}

	// 按 key 执行 reduce 函数，结果写入 buf 中
	var buf bytes.Buffer
	for key, values := range results {
		output := reduceF(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}

	// 将 reduce 结果写入文件
	atomicWriteFile(generateReduceResultFileName(response.Id), &buf)

	// 上报 reduce(id) 任务完成
	doReport(response.Id, ReducePhase)
}

// 发送心跳给 Coordinator
func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

// 发送任务状态给 Coordinator
func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func call(rpcName string, args interface{}, reply interface{}) bool {
	// 获取域套接字
	sockName := coordinatorSock()

	// 建立连接
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// 发送请求
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	// 出错返回
	return false
}
