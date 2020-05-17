package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply, success := AskNewTask()
		if !success {
			return
		}
		switch reply.NewTask.Type {
		case MAP:
			//handle map task
			var intermediate []KeyValue
			for _, filename := range reply.NewTask.FileList {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}
			fileList := saveIntermediateFiles(intermediate, reply.NewTask.Id, reply.NumReduce)
			newReply, success := TaskComplete(RequestArgs{MAP_TASK_COMPLETE, reply.NewTask.Id, fileList})
			if !success {
				return
			}
			if newReply.NewTask.Type == CLOSE {
				return
			}

		case REDUCE:
			//handle reduce task
			fileName := "mr-out-" + strconv.Itoa(reply.NewTask.Id)
			outputFile, err:= os.Create(fileName)
			if err != nil {
				log.Fatal("cannot output to %v", fileName)
			}

			keyToPairs := readIntermediateFiles(reply.NewTask.FileList)
			for key, pairs := range keyToPairs {
				output := reducef(key, pairs)
				fmt.Fprintf(outputFile, "%v %v\n", key, output)
			}
			outputFile.Close()

			newReply, success := TaskComplete(RequestArgs{REDUCE_TASK_COMPLETE, reply.NewTask.Id, nil})
			if !success {
				return
			}
			if newReply.NewTask.Type == CLOSE {
				return
			}
		case CLOSE:
			//close server
			return
		}
	}
}

// save key-value pairs to intermediate files
func saveIntermediateFiles(pairs []KeyValue, id int, numReduce int) []string {
	var fileList []string
	encoders := make(map[int]*json.Encoder)
	for i := 0; i < numReduce; i++ {
		fileName := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		fileList = append(fileList, fileName)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatal("File Creation:", err)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		encoders[i] = enc
	}
	for _, pair := range pairs {
		reduceId := ihash(pair.Key) % numReduce
		err := encoders[reduceId].Encode(&pair)
		if err != nil {
			log.Fatal("Json Encode:", err)
		}
	}
	return fileList
}

// read key-value pairs from intermediate files
func readIntermediateFiles(fileList []string) map[string][]string {
	keyToPairs := make(map[string][]string)
	var decoders []*json.Decoder
	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("File Creation:", err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		decoders = append(decoders, dec)
	}
	for _, decoder := range decoders {
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			keyToPairs[kv.Key] = append(keyToPairs[kv.Key], kv.Value)
		}
	}
	return keyToPairs
}

//
// the RPC argument and reply types are defined in rpc.go.
//
func AskNewTask() (RequestReply, bool) {
	args := RequestArgs{}
	args.Type = ASK_NEW_TASK
	reply := RequestReply{}
	// send the RPC request, wait for the reply.
	success := call("Master.Request", &args, &reply)
	return reply, success
}

func TaskComplete(args RequestArgs) (RequestReply, bool) {
	reply := RequestReply{}
	// send the RPC request, wait for the reply.
	success := call("Master.Request", &args, &reply)
	return reply, success
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
