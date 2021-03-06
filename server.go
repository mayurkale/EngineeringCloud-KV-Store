package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//Below struct acts as value in the key-value pair of store
type mapval struct {
	expirytime int
	version    int64
	numbytes   int
	value      string
	timestamp  int64
}

//Below struct is used as node in heap which is maintained to perform delete operations on expired keys

type exp_struct struct {
	value    string
	priority int64
	init_ts  int64
	index    int
}

type PriorityQueue []*exp_struct

/*
memmap is the key value store
*/
var memmap = make(map[string]mapval)

var mutex = &sync.RWMutex{}

/*
Below heap is used to store key and there corresponding expiry time as a MIN-HEAP
*/
var exp_heap = make(PriorityQueue, 0)

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {

	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*exp_struct)
	item.index = n
	*pq = append(*pq, item)
	heap.Fix(pq, item.index)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// update_node modifies the priority and value of an exp_struct in the queue.

func (pq *PriorityQueue) update_node(item *exp_struct, value string, priority int64) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

/*
periodic_expiry_check() is invoked after every 5sec to determine if any of the key-val pair is expired. This function actually pops value from heap to check the most likely expired key-val pair
*/
func periodic_expiry_check() {

	ticker := time.NewTicker(time.Millisecond * 5000)
	go func() {
		for range ticker.C {
			if exp_heap.Len() > 0 {

				mutex.Lock()
				item := exp_heap[0]
				val, ok := memmap[item.value]

				for item.priority < time.Now().Unix() {

					if ok == true && item.init_ts == (val.timestamp-int64(val.expirytime)) {

						delete(memmap, item.value)
					}

					item = heap.Pop(&exp_heap).(*exp_struct)

					if exp_heap.Len() > 0 {
						item = exp_heap[0]
					} else {
						break

					}
				}

				mutex.Unlock()
			}
		}
	}()

	select {}

}

/*

handleconnection(): for each TCP connection this function parses command from client and sends appropriate reply
*/

func handleconnection(con net.Conn) {

	var response string
	var read = true

	reader := bufio.NewReader(con)

	for read {

		data, error := reader.ReadBytes('\n')

		if error == io.EOF {
			message := "ERR_INTERNAL\r\n"
			io.Copy(con, bytes.NewBufferString(message))
			break
		}

		if error != nil {
			message := "ERR_INTERNAL\r\n"
			io.Copy(con, bytes.NewBufferString(message))
			break
		}

		response = string(data)
		//fmt.Print("FIrst: ",response)

		var res []string
		res = strings.Split((response), " ")

		switch res[0] {

		case "set":
			var key string
			var numbytes int
			var expirytime int
			var value string
			var cmd_err bool
			cmd_err = false
			if len(res) != 5 && len(res) != 4 {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			}

			if res[1] == "" {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
			} else {
				key = strings.TrimSpace(res[1])

				var reply_flag bool
				reply_flag = true
				if len(res) == 5 {
					if res[4] != "noreply\r\n" {
						message := "ERRCMDERR\r\n"
						io.Copy(con, bytes.NewBufferString(message))
						break
					} else {
						reply_flag = false
					}
				}

				if len(key) > 250 {

					cmd_err = true
				}

				if _, err := strconv.Atoi(res[2]); err == nil {

					temp_expirytime, _ := strconv.Atoi(res[2])

					if temp_expirytime >= 0 {
						expirytime = temp_expirytime
					} else {
						cmd_err = true
					}

				} else {
					cmd_err = true
				}
				temp_str := strings.TrimSpace(res[3])

				if _, errn := strconv.Atoi(temp_str); errn == nil {

					temp_numbytes, _ := strconv.Atoi(temp_str)

					if temp_numbytes > 0 {
						numbytes = temp_numbytes
					} else {
						cmd_err = true
					}
				} else {
					cmd_err = true
				}

				data, error := reader.ReadBytes('\n')
				if error != nil {
					message := "ERR_INTERNAL\r\n"
					io.Copy(con, bytes.NewBufferString(message))
					break
				}
				value = string(data)

				if len(value) == (numbytes + 2) {

					value = strings.TrimSpace(value)

					if cmd_err == true {
						if reply_flag == true {
							message := "ERRCMDERR\r\n"
							io.Copy(con, bytes.NewBufferString(message))

						}
						break

					}

					mutex.Lock()

					_, ok := memmap[key]
					cur_ts := time.Now().Unix()
					new_exp := cur_ts + int64(expirytime)
					if ok == true {
						new_version := memmap[key].version
						new_version = new_version + 1

						memmap[key] = mapval{expirytime, new_version, numbytes, value, new_exp}

					} else {

						memmap[key] = mapval{expirytime, 0, numbytes, value, new_exp}

					}

					//Below if will add key to expiry heap

					if expirytime != 0 {

						item := &exp_struct{
							value:    key,
							priority: new_exp,
							init_ts:  cur_ts,
						}

						heap.Push(&exp_heap, item)

					}

					if reply_flag == true {
						message := "OK "
						message = message + strconv.FormatInt(memmap[key].version, 10) + "\r\n"

						io.Copy(con, bytes.NewBufferString(message))
					}

					mutex.Unlock()
				} else {
					if reply_flag == true {
						message := "ERRCMDERR\r\n"
						io.Copy(con, bytes.NewBufferString(message))
					}
					break

				}

			}
		case "get":

			if len(res) != 2 {
				message := "ERRCMDERR\r\n"
				con.Write([]byte(message))
				break
			}

			req_key := strings.TrimSpace(res[1])

			mutex.RLock()
			value, ok := memmap[req_key]
			mutex.RUnlock()

			if ok == false || value.timestamp < time.Now().Unix() {
				message := "ERRNOTFOUND\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			} else {
				message := "VALUE "
				message = message + strconv.Itoa(value.numbytes)
				message = message + "\r\n"
				message = message + value.value + "\r\n"
				io.Copy(con, bytes.NewBufferString(message))
			}

		case "getm":

			if len(res) != 2 {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			}

			req_key := strings.TrimSpace(res[1])

			mutex.RLock()
			value, ok := memmap[req_key]
			mutex.RUnlock()

			if ok == false || value.timestamp < time.Now().Unix() {
				message := "ERRNOTFOUND\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			} else {

				var diff_expiry int64
				diff_expiry = (value.timestamp - time.Now().Unix())
				message := "VALUE" + " " + strconv.FormatInt(value.version, 10) + " " + strconv.FormatInt(diff_expiry, 10) + " " + strconv.Itoa(value.numbytes) + "\r\n"

				message = message + value.value + "\r\n"
				io.Copy(con, bytes.NewBufferString(message))
			}

		case "cas":

			var key string
			var numbytes int
			var version int64
			var expirytime int
			var value string
			var err_cmd bool
			err_cmd = false

			if len(res) != 5 && len(res) != 6 {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			}

			var reply_flag bool
			reply_flag = true
			if len(res) == 6 {
				if res[5] != "noreply\r\n" {
					message := "ERRCMDERR\r\n"
					io.Copy(con, bytes.NewBufferString(message))
					break
				} else {
					reply_flag = false
				}
			}

			if res[1] == "" {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
			} else {
				key = strings.TrimSpace(res[1])

				if len(key) > 250 {
					err_cmd = true
				}
				if _, err := strconv.Atoi(res[2]); err == nil {

					temp_expirytime, _ := strconv.Atoi(res[2])

					if temp_expirytime >= 0 {
						expirytime = temp_expirytime
					} else {
						err_cmd = true
					}
				} else {
					err_cmd = true
				}

				if _, err := strconv.ParseInt(res[3], 10, 64); err == nil {

					temp_version, _ := strconv.ParseInt(res[3], 10, 64)

					if temp_version >= 0 {
						version = temp_version
					} else {
						err_cmd = true
					}

				} else {
					err_cmd = true
				}
				temp_str := strings.TrimSpace(res[4])

				if _, errn := strconv.Atoi(temp_str); errn == nil {

					temp_numbytes, _ := strconv.Atoi(temp_str)
					if temp_numbytes > 0 {
						numbytes = temp_numbytes
					} else {
						err_cmd = true
					}
				} else {
					err_cmd = true
				}

				data, error := reader.ReadBytes('\n')

				if error != nil {
					message := "ERR_INTERNAL\r\n"
					io.Copy(con, bytes.NewBufferString(message))
					break
				}
				value = string(data)

				if err_cmd == true {
					message := "ERRCMDERR\r\n"
					io.Copy(con, bytes.NewBufferString(message))
					break

				}

				if len(value) == (numbytes + 2) {

					value = strings.TrimSpace(value)
					mutex.Lock()
					_, ok := memmap[key]

					if ok == true {
						if memmap[key].version == version {

							if memmap[key].timestamp < time.Now().Unix() {
								if reply_flag == true {
									message := "ERRNOTFOUND\r\n"
									io.Copy(con, bytes.NewBufferString(message))
								}

								mutex.Unlock()
								break

							}
							cur_ts := time.Now().Unix()
							new_exp := time.Now().Unix() + int64(expirytime)
							memmap[key] = mapval{expirytime, memmap[key].version + 1, numbytes, value, new_exp}

							//Below if will add key to expiry heap

							if expirytime != 0 {

								item := &exp_struct{
									value:    key,
									priority: new_exp,
									init_ts:  cur_ts,
								}

								heap.Push(&exp_heap, item)

							}
						} else {
							if reply_flag == true {
								message := "ERR_VERSION\r\n"
								io.Copy(con, bytes.NewBufferString(message))
							}

							mutex.Unlock()
							break

						}

					} else {
						if reply_flag == true {
							message := "ERRNOTFOUND\r\n"
							io.Copy(con, bytes.NewBufferString(message))
						}

						mutex.Unlock()
						break

					}

					if reply_flag == true {
						message := "OK "
						message = message + strconv.FormatInt(memmap[key].version, 10) + "\r\n"

						io.Copy(con, bytes.NewBufferString(message))
						mutex.Unlock()
						break

					}
				} else {

					if reply_flag == true {
						message := "ERRCMDERR\r\n"
						io.Copy(con, bytes.NewBufferString(message))
					}
				}

			}
		case "delete":

			if len(res) != 2 {
				message := "ERRCMDERR\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				break
			}

			req_key := strings.TrimSpace(res[1])

			mutex.Lock()
			_, ok := memmap[req_key]
			if ok == false || memmap[req_key].timestamp < time.Now().Unix() {
				message := "ERRNOTFOUND\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				mutex.Unlock()
				break
			} else {

				delete(memmap, req_key)
				message := "DELETED\r\n"
				io.Copy(con, bytes.NewBufferString(message))
				mutex.Unlock()
			}

		default:
			message := "ERRCMDERR\r\n"
			io.Copy(con, bytes.NewBufferString(message))

		}

	}

	con.Close()
}

func main() {
	var (
		host   = "127.0.0.1"
		port   = "9000"
		remote = host + ":" + port
	)
	lis, error := net.Listen("tcp", remote)
	defer lis.Close()
	if error != nil {
		os.Exit(1)
	}

	heap.Init(&exp_heap)
	go periodic_expiry_check()
	for {

		con, error := lis.Accept()
		if error != nil {
			fmt.Printf("INT_ERR: Accepting data: %s\n", error)
			continue
		}
		go handleconnection(con)
	}
}
