package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func generateRandomNumber() int {
	// 使用crypto/rand生成一个随机数
	rand.Seed(time.Now().UnixNano())
	// 生成一个 [min, max] 范围内的随机正整数
	min := 1
	max := 100000
	randomNum := rand.Intn(max-min) + min
	return randomNum
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	logFile, _ := os.OpenFile("/home/lky/school/mit/6.5840/src/main/terminal.txt",
		os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	log.SetOutput(logFile)
	workId := generateRandomNumber()
	for {
		response := JobResponse{}
		request := JobRequest{WorkId: workId}
		call("Coordinator.JobHeartbeat", &request, &response)
		log.Printf("Worker%v: get coordinator's heartbeat %v \n", workId, response)
		switch response.JobType {
		case MapJob:
			log.Printf("Map job %v", response.Id)
			mapTaskFunc(mapf, &response)
			call("Coordinator.Report", &ReportRequest{response.Id, MapPhase}, &ReportResponse{})
		case ReduceJob:
			log.Printf("Reduce job %v", response.Id)
			ReduceTaskfunc(reducef, &response)
			call("Coordinator.Report", &ReportRequest{response.Id, ReducePhase}, &ReportResponse{})
		case WaitJob:
			log.Printf("Wait job %v", response.Id)
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		}
	}
}

func mapTaskFunc(mapf func(string, string) []KeyValue, response *JobResponse) {
	filePath := response.FilePath
	file, _ := os.Open(filePath)
	content, _ := io.ReadAll(file)
	file.Close()
	kva := mapf(filePath, string(content))
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	log.Printf("intermediates' len:%v\n", len(intermediates))
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			log.Printf("worker create a temp file: %v\n", intermediateFilePath)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			writeFile(intermediateFilePath, &buf)
			wg.Done()
		}(index, intermediate)
	}
	wg.Wait()
}

func ReduceTaskfunc(reducef func(string, []string) string, response *JobResponse) {
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
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
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	var buf bytes.Buffer
	for key, values := range results {
		output := reducef(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}
	writeFile(generateReduceResultFileName(response.Id), &buf)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
