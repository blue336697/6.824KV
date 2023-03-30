package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map 函数返回 KeyValue 的一部分。
//

type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 为 Map 发出的每个 KeyValue 选择 reduce 任务编号。
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	keepFlag := true
	for keepFlag {
		task := GetTaskFromMaster() //这个方法就是得到任务的方法
		switch task.TaskType {      //会根据任务得到不同的回应
		case MapTask:
			{
				DoMapTask(mapf, &task) //如果是map函数我们就执行并得出结果
				callDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task) //如果是map函数我们就执行并得出结果
				callDone(&task)
			}
		case WaitingTask: //没任务了，自旋个一会等别的任务
			{
				fmt.Println("所有任务都在进行中，请稍候...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("任务 :[", task.TaskId, "] 被终止...")
				keepFlag = false
			}

		}
	}
}

// DoMapTask 这个函数就会根据分发过来的任务，去具体执行。可以参考给定的wc.go、mr/sequential.go的map方法
// 两个参数分别是任务名，以及任务的内容
func DoMapTask(mapf func(string, string) []KeyValue, t *Task) {
	var kvs []KeyValue
	fileName := t.FileInput[0]
	file, err := os.Open(fileName) //根据文件名打开文件
	if err != nil {
		log.Fatalf("不能打开 %v", fileName)
	}
	//通过io得到文件的内容
	fileContent, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("不能读取 %v", fileName)
	}
	file.Close()
	kvs = mapf(fileName, string(fileContent)) // map返回一组KV结构体数组
	buckets := t.ReducerNum                   //我们要将kv进行分类，查看分类的数量，放入指定的桶中

	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, buckets) //一维hash编号，二维具体kv结构
	for _, kv := range kvs {                //按编号归位
		HashedKV[ihash(kv.Key)%buckets] = append(HashedKV[ihash(kv.Key)%buckets], kv)
	}

	for i := 0; i < buckets; i++ {
		//构造输出结果文件,文件个格式是json，名字格式：mr-tmp-x-y
		//strconv.Itoa()方法返回数字对应的小写字母
		oname := "mr-tmp-" + strconv.Itoa(t.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile) //通过json编码
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}

}

// 我们在做reduce任务时，其实还要负责将这些kv结构把全部k相同的输出到一个文件中
func DoReduceTask(reducef func(string, []string) string, t *Task) {
	fileId := t.TaskId
	sortRes := sortTask(t.FileInput)
	dir, _ := os.Getwd()                              //读取全部路径
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*") //通过通配符的方式读取全部map生成的持久化文件
	if err != nil {
		log.Fatal("创建临时文件失败", err)
	}
	i := 0
	for i < len(sortRes) {
		j := i + 1
		//我们寻找不同key的分割线
		for j < len(sortRes) && sortRes[j].Key == sortRes[i].Key {
			j++
		}
		//然后找到分割线 ，将当前下key对应的所有的value进行合并
		var values []string
		for k := i; k < j; k++ {
			values = append(values, sortRes[k].Value)
		}
		//通过参数函数，将最后结果合并成一个文件
		mergeFile := reducef(sortRes[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", sortRes[i].Key, mergeFile)
		i = j
	}
	tempFile.Close()
	//将旧文件用新文件进行覆盖
	fn := fmt.Sprintf("mr-out-%d", fileId)
	os.Rename(tempFile.Name(), fn)
}

// 根据map持久化的文件，进行排序根据key的字符进行字符集排序
func sortTask(files []string) []KeyValue {
	var kvs []KeyValue
	//遍历传过来的文件
	for _, fileName := range files {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file) //通过json解码
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil { //循环取结构
				break
			}
			kvs = append(kvs, kv) //将kv都添加到kv的数组中
		}
		file.Close()
	}
	//对kv数据进行字符集升序排序
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	return kvs
}

// 做完任务也需要调用rpc在协调者中将任务状态为设为已完成，
// 以方便协调者确认任务已完成，worker与协调者程序能正常退出。
func callDone(t *Task) Task {
	//通过call函数得到rpc调用的回应值并得到任务列表
	args := t
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("Master标记失败!\n")
	}
	return reply
}

// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTaskFromMaster() Task {
	//通过call函数得到rpc调用的回应值并得到任务列表
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("向Master请求任务失败!\n")
	}
	return reply
}

// 向协调器发送 RPC 请求，等待响应。通常返回真。如果出现问题，则返回 false。
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
