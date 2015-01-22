package main

import (
	"container/heap"
	"fmt"
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
	index    int
}

type PriorityQueue []*exp_struct

/*
memmap is the key value store
*/
var memmap = make(map[string]mapval)

var mutex = &sync.RWMutex{}
var hmutex = &sync.RWMutex{}

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

				hmutex.Lock()
				item := exp_heap[0]
				for item.priority >= time.Now().Unix() {

					item = heap.Pop(&exp_heap).(*exp_struct)

					mutex.Lock()
					delete(memmap, item.value)
					mutex.Unlock()

					fmt.Printf("%.2d:%s ", item.priority, item.value)
					if exp_heap.Len() > 0 {
						item = exp_heap[0]
					} else {
						break

					}
				}

				hmutex.Unlock()
			}
		}
	}()

	select {}

}

/*

handleconnection(): for each TCP connection this function parses command from client and sends appropriate reply

*/
func handleconnection(con net.Conn) {
	var (
		data = make([]byte, 1024)
	)
	var response string
	var read = true
	for read {
		n, error := con.Read(data)

		if error != nil {
			message := "ERR_INTERNAL\r\n"
			con.Write([]byte(message))
		}

		response = string(data[0:n])
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
				con.Write([]byte(message))
				break
			}

			if res[1] == "" {
				message := "ERRCMDERR\r\n"
				con.Write([]byte(message))
			} else {
				key = strings.TrimSpace(res[1])

				var reply_flag bool
				reply_flag = true
				if len(res) == 5 {
					if res[4] != "noreply\r\n" {
						message := "ERRCMDERR\r\n"
						con.Write([]byte(message))
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

				n, error := con.Read(data)
				if error != nil {
					message := "ERR_INTERNAL\r\n"
					con.Write([]byte(message))
					break
				}
				value = string(data[0:n])

				if n == (numbytes + 2) {

					value = strings.TrimSpace(value)

					if cmd_err == true {
						if reply_flag == true {
							message := "ERRCMDERR\r\n"
							con.Write([]byte(message))
						}
						break

					}

					mutex.Lock()

					_, ok := memmap[key]

					if ok == true {
						new_version := memmap[key].version
						new_version = new_version + 1

						new_exp := time.Now().Unix() + int64(expirytime)
						memmap[key] = mapval{expirytime, new_version, numbytes, value, new_exp}

						if expirytime != 0 {
							hmutex.Lock()
							n := exp_heap.Len()
							for i := 0; i < n; i = i + 1 {

								if exp_heap[i].value == key {
									item := exp_heap[i]

									exp_heap.update_node(item, item.value, new_exp)
									hmutex.Unlock()
									break

								}
							}

						} else {
							hmutex.Lock()
							n1 := exp_heap.Len()
							for i := 0; i < n1; i = i + 1 {
								if exp_heap[i].value == key {
									heap.Remove(&exp_heap, i)
									hmutex.Unlock()
									break
								}
							}

						}

					} else {

						new_exp := time.Now().Unix() + int64(expirytime)
						memmap[key] = mapval{expirytime, 0, numbytes, value, new_exp}
						fmt.Println(" Key value is ", key)

						//TODO: calculate remaining time in CAS
						//Below if will add key to expiry heap

						if expirytime != 0 {

							item := &exp_struct{
								value:    key,
								priority: new_exp,
							}
							hmutex.Lock()
							heap.Push(&exp_heap, item)
							hmutex.Unlock()
						}

					}
					if reply_flag == true {
						message := "OK "
						message = message + strconv.FormatInt(memmap[key].version, 10) + "\r\n"

						con.Write([]byte(message))
					}

					mutex.Unlock()
				} else {
					if reply_flag == true {
						message := "ERRCMDERR\r\n"
						con.Write([]byte(message))
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
				con.Write([]byte(message))
				break
			} else {
				message := "VALUE "
				message = message + strconv.Itoa(value.numbytes)
				message = message + "\r\n"
				message = message + value.value + "\r\n"
				con.Write([]byte(message))
			}

		case "getm":

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
				con.Write([]byte(message))
				break
			} else {

				var diff_expiry int64
				diff_expiry = (value.timestamp - time.Now().Unix())
				message := "VALUE" + " " + strconv.FormatInt(value.version, 10) + " " + strconv.FormatInt(diff_expiry, 10) + " " + strconv.Itoa(value.numbytes) + "\r\n"

				message = message + value.value + "\r\n"
				con.Write([]byte(message))
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
				con.Write([]byte(message))
				break
			}

			var reply_flag bool
			reply_flag = true
			if len(res) == 6 {
				if res[5] != "noreply\r\n" {
					message := "ERRCMDERR43\r\n"
					con.Write([]byte(message))
					break
				} else {
					reply_flag = false
				}
			}

			if res[1] == "" {
				message := "ERRCMDERR\r\n"
				con.Write([]byte(message))
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

					temp_numbytes, _ := strconv.Atoi(res[2])

					if temp_numbytes > 0 {
						numbytes = temp_numbytes
					} else {
						err_cmd = true
					}
				} else {
					err_cmd = true
				}

				n, error := con.Read(data)
				if error != nil {
					message := "ERR_INTERNAL\r\n"
					con.Write([]byte(message))
					break
				}
				value = string(data[0:n])

				if err_cmd == true {
					message := "ERRCMDERR\r\n"
					con.Write([]byte(message))
					break

				}

				if n == (numbytes + 2) {

					value = strings.TrimSpace(value)
					mutex.Lock()
					_, ok := memmap[key]

					if ok == true {
						if memmap[key].version == version {

							if memmap[key].timestamp < time.Now().Unix() {
								if reply_flag == true {
									message := "ERRNOTFOUND\r\n"
									con.Write([]byte(message))
								}

								mutex.Unlock()
								break

							}

							new_exp := time.Now().Unix() + int64(expirytime)
							memmap[key] = mapval{expirytime, memmap[key].version + 1, numbytes, value, new_exp}

							if expirytime != 0 {

								hmutex.Lock()
								n := exp_heap.Len()
								for i := 0; i < n; i = i + 1 {

									if exp_heap[i].value == key {
										item := exp_heap[i]
										exp_heap.update_node(item, item.value, new_exp)
										hmutex.Unlock()
										break

									}
								}

							} else {
								hmutex.Lock()
								n1 := exp_heap.Len()
								for i := 0; i < n1; i = i + 1 {
									if exp_heap[i].value == key {
										heap.Remove(&exp_heap, i)
										hmutex.Unlock()
										break
									}
								}

							}
						} else {
							if reply_flag == true {
								message := "ERRCMDERR\r\n"
								con.Write([]byte(message))
							}

							mutex.Unlock()
							break

						}

					} else {
						if reply_flag == true {
							message := "ERRCMDERR\r\n"
							con.Write([]byte(message))
						}

						mutex.Unlock()
						break

					}

					if reply_flag == true {
						message := "OK "
						message = message + strconv.FormatInt(memmap[key].version, 10) + "\r\n"

						con.Write([]byte(message))
						mutex.Unlock()
						break

					}
				} else {

					if reply_flag == true {
						message := "ERRCMDERR\r\n"
						con.Write([]byte(message))
					}
				}

			}
		case "delete":

			if len(res) != 2 {
				message := "ERRCMDERR\r\n"
				con.Write([]byte(message))
				break
			}

			req_key := strings.TrimSpace(res[1])

			mutex.Lock()
			_, ok := memmap[req_key]
			if ok == false || memmap[req_key].timestamp < time.Now().Unix() {
				message := "ERRNOTFOUND\r\n"
				con.Write([]byte(message))
				mutex.Unlock()
				break
			} else {

				delete(memmap, req_key)
				hmutex.Lock()
				n1 := exp_heap.Len()
				for i := 0; i < n1; i = i + 1 {
					if exp_heap[i].value == req_key {

						heap.Remove(&exp_heap, i)
						hmutex.Unlock()
						break
					}
				}

				message := "DELETED\r\n"
				con.Write([]byte(message))
				mutex.Unlock()
			}

		default:
			message := "ERRCMDERR\r\n"
			con.Write([]byte(message))

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
