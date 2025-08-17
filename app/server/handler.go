package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const NEVER_EXPIRED = -1

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		commands, err := parseCommands(conn)
		if err != nil {
			conn.Write([]byte("-ERR parsing command\r\n"))
			return
		}
		if len(commands) == 0 {
			continue
		}

		cmd := strings.ToUpper(commands[0])
		args := commands[1:]

		switch cmd {
		case "PING":
			s.handlePing(conn)
		case "ECHO":
			s.handleEcho(conn, args)
		case "SET", "GET", "INCR":
			s.handleStringCommand(conn, cmd, args)
		case "RPUSH", "LPUSH", "LPOP", "BLPOP", "LRANGE", "LLEN":
			s.handleListCommand(conn, cmd, args)
		case "XADD", "XRANGE", "XREAD":
			s.handleStreamCommand(conn, cmd, args)
		case "TYPE":
			s.handleTypeCommand(conn, args)
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func (s *Server) handlePing(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}
func (s *Server) handleEcho(conn net.Conn, args []string) {
	if len(args) >= 1 {
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])))
	}
}
func (s *Server) handleStringCommand(conn net.Conn, cmd string, args []string) {
	switch cmd {
	case "SET":
		if len(args) == 2 {
			s.SafeMap.Set(args[0], args[1], NEVER_EXPIRED)
			conn.Write([]byte("+OK\r\n"))
		} else if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
			px, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR invalid PX value\r\n"))
				return
			}
			s.SafeMap.Set(args[0], args[1], px)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'SET'\r\n"))
		}
	case "GET":
		if len(args) != 1 {
			conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
			return
		}
		value, ok := s.SafeMap.Get(args[0])
		if ok {
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
		} else {
			conn.Write([]byte("$-1\r\n"))
		}
	case "INCR":
		if len(args) != 1 {
			conn.Write([]byte("-ERR wrong number of arguments for 'INCR'\r\n"))
			return
		}
		newVal, ok, errStr := s.SafeMap.Incr(args[0])
		if ok {
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", newVal)))
		} else {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", errStr)))
		}
	}
}
func (s *Server) handleListCommand(conn net.Conn, cmd string, args []string) {
	switch cmd {
	case "RPUSH":
		if len(args) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments for RPUSH\r\n"))
			return
		}
		count := s.SafeList.RPush(args[0], args[1:]...)
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))

	case "LPUSH":
		if len(args) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments for LPUSH\r\n"))
			return
		}
		count := s.SafeList.LPush(args[0], args[1:]...)
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))

	case "LPOP":
		if len(args) < 1 {
			conn.Write([]byte("-ERR wrong number of arguments for LPOP\r\n"))
			return
		}
		popCount := 1
		if len(args) >= 2 {
			n, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR invalid pop count\r\n"))
				return
			}
			popCount = n
		}
		items, ok := s.SafeList.LPop(args[0], popCount)
		if !ok || len(items) == 0 {
			conn.Write([]byte("$-1\r\n"))
			return
		}
		if popCount == 1 {
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(items[0]), items[0])))
		} else {
			sb := strings.Builder{}
			sb.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
			for _, v := range items {
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
			}
			conn.Write([]byte(sb.String()))
		}

	case "BLPOP":
		if len(args) != 2 {
			conn.Write([]byte("-ERR wrong number of arguments for BLPOP\r\n"))
			return
		}
		timeoutSec, err := strconv.ParseFloat(args[1], 64)
		if err != nil || timeoutSec < 0 {
			conn.Write([]byte("-ERR invalid timeout\r\n"))
			return
		}
		val, ok := s.SafeList.BLPop(args[0], time.Duration(timeoutSec*float64(time.Second)))
		if !ok {
			conn.Write([]byte("$-1\r\n"))
			return
		}
		sb := strings.Builder{}
		sb.WriteString("*2\r\n")
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0]))
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		conn.Write([]byte(sb.String()))

	case "LRANGE":
		if len(args) != 3 {
			conn.Write([]byte("-ERR wrong number of arguments for LRANGE\r\n"))
			return
		}
		start, err1 := strconv.Atoi(args[1])
		stop, err2 := strconv.Atoi(args[2])
		if err1 != nil || err2 != nil {
			conn.Write([]byte("-ERR invalid start or stop index\r\n"))
			return
		}
		items := s.SafeList.LRange(args[0], start, stop)
		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
		for _, v := range items {
			sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		}
		conn.Write([]byte(sb.String()))

	case "LLEN":
		if len(args) != 1 {
			conn.Write([]byte("-ERR wrong number of arguments for LLEN\r\n"))
			return
		}
		count := s.SafeList.LLen(args[0])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
	}
}

func (s *Server) handleStreamCommand(conn net.Conn, cmd string, args []string) {
	switch cmd {
	case "XADD":
		if len(args) < 3 {
			conn.Write([]byte("-ERR wrong number of arguments for XADD\r\n"))
			return
		}
		fields := make(map[string]interface{})
		for i := 2; i < len(args); i += 2 {
			if i+1 < len(args) {
				fields[args[i]] = args[i+1]
			}
		}
		id, ok, errStr := s.SafeStream.XAdd(args[0], args[1], fields)
		if ok {
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
		} else {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", errStr)))
		}
	case "XRANGE":
		if len(args) != 3 {
			conn.Write([]byte("-ERR wrong number of arguments for XRANGE\r\n"))
			return
		}
		items, ok := s.SafeStream.XRange(args[0], args[1], args[2])
		if !ok {
			conn.Write([]byte("*0\r\n"))
			return
		}
		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
		for _, item := range items {
			sb.WriteString("*2\r\n")
			sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Id), item.Id))
			sb.WriteString(fmt.Sprintf("*%d\r\n", len(item.Fields)*2))
			for k, v := range item.Fields {
				valStr := toRespString(v.Value)
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr))
			}
		}
		conn.Write([]byte(sb.String()))
	case "XREAD":
		// TODO: implement XREAD
		conn.Write([]byte("-ERR XREAD not implemented yet\r\n"))
	}
}

func (s *Server) handleTypeCommand(conn net.Conn, args []string) {
	if len(args) != 1 {
		conn.Write([]byte("-ERR wrong number of arguments for TYPE\r\n"))
		return
	}

	var typeName string
	var ok bool
	key := args[0]

	if typeName, ok = s.SafeMap.Type(key); ok {
	} else if typeName, ok = s.SafeList.Type(key); ok {
	} else if typeName, ok = s.SafeStream.Type(key); ok {
	} else {
		typeName = "none"
	}

	conn.Write([]byte(fmt.Sprintf("+%s\r\n", typeName)))
}

// func handleConnection(conn net.Conn) {
// 	defer conn.Close()

// 	scanner := bufio.NewScanner(conn)

// 	var commands []string
// 	var readBulkCommand bool
// 	var isReadFirstByte bool
// 	var command_count int
// 	var err error
// 	for scanner.Scan() {
// 		text := scanner.Text()

// 		// Handle the command
// 		text = strings.TrimSpace(text)
// 		// fmt.Println(text)

// 		if strings.HasPrefix(text, "*") && !isReadFirstByte {

// 			command_count, err = strconv.Atoi(text[1:])
// 			if err != nil {
// 				conn.Write([]byte("-ERR invalid number of arguments\r\n"))
// 				continue
// 			}

// 			if command_count == 0 {
// 				conn.Write([]byte("-ERR empty command\r\n"))
// 				continue
// 			}
// 			isReadFirstByte = true
// 			commands = make([]string, 0, command_count)

// 		} else if strings.HasPrefix(text, "$") {
// 			//["ECHO", "hey"]
// 			//*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
// 			readBulkCommand = true
// 			if len(text) > 1 {
// 				continue
// 			}
// 		}

// 		// $1 應該跳過
// 		// $  不應該跳過

// 		if readBulkCommand && isReadFirstByte {
// 			commands = append(commands, text)
// 			readBulkCommand = false
// 		}

// 		if len(commands) == command_count {

// 			// fmt.Printf("commands %v\r\n", commands)

// 			switch strings.ToUpper(commands[0]) {
// 			case "PING":
// 				conn.Write([]byte("+PONG\r\n"))
// 			case "ECHO":
// 				if len(commands) >= 2 {
// 					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(commands[1]), commands[1])))
// 				}
// 				// } else {
// 				// 	conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
// 				// }
// 			case "SET":
// 				if len(commands) == 3 {
// 					safeMap.Set(commands[1], commands[2], NEVER_EXPIRED)
// 					conn.Write([]byte("+OK\r\n"))
// 				} else if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
// 					px, err := strconv.ParseInt(commands[4], 10, 64)
// 					if err != nil {
// 						conn.Write([]byte("-ERR invalid PX value\r\n"))
// 						continue
// 					}
// 					safeMap.Set(commands[1], commands[2], px)
// 					conn.Write([]byte("+OK\r\n"))

// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
// 				}
// 			case "GET":
// 				if len(commands) >= 2 {
// 					value, ok := safeMap.Get(commands[1])
// 					if ok {
// 						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
// 					} else {
// 						conn.Write([]byte("$-1\r\n")) // nil response for non-existing key
// 					}
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
// 				}
// 			case "RPUSH":
// 				if len(commands) >= 3 {
// 					v := safeList.RPush(commands[1], commands[2:]...)
// 					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
// 				}
// 			case "LPUSH":
// 				if len(commands) >= 3 {
// 					v := safeList.LPush(commands[1], commands[2:]...)
// 					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))

// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
// 				}
// 			case "LPOP":
// 				if len(commands) >= 2 {
// 					popCount := 1
// 					if len(commands) == 3 {
// 						popCount, err = strconv.Atoi(commands[2])
// 						if err != nil {
// 							conn.Write([]byte("-ERR invalid popCount\r\n"))
// 							continue
// 						}
// 					}
// 					v, ok := safeList.LPop(commands[1], popCount)
// 					if ok && len(v) > 0 {
// 						if popCount == 1 {
// 							conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v[0]), v[0])))
// 						} else {
// 							stringBuilder := strings.Builder{}
// 							stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(v)))
// 							for _, value := range v {
// 								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
// 							}
// 							conn.Write([]byte(stringBuilder.String()))
// 						}
// 					} else {
// 						conn.Write([]byte("$-1\r\n")) // nil response for non-existing key or empty list
// 					}
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
// 				}
// 			case "BLPOP":
// 				if len(commands) == 3 {
// 					// BLPOP key timeout
// 					// timeout is in milliseconds
// 					timeout, err := strconv.ParseFloat(commands[2], 64)
// 					if err != nil {
// 						conn.Write([]byte("-ERR invalid timeout value\r\n"))
// 						continue
// 					}
// 					value, ok := safeList.BLPop(commands[1], time.Duration(timeout*float64(time.Second)))
// 					if ok {
// 						key := commands[1]
// 						conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)))
// 					} else {
// 						conn.Write([]byte("$-1\r\n"))
// 					}
// 				} else {
// 					conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
// 				}
// 			case "LRANGE":
// 				if len(commands) == 4 {
// 					start, err := strconv.Atoi(commands[2])
// 					stop, err := strconv.Atoi(commands[3])
// 					if err != nil {
// 						conn.Write([]byte("-ERR invalid start index or end index\r\n"))
// 						continue
// 					}

// 					v := safeList.LRange(commands[1], start, stop)
// 					v_len := len(v)

// 					stringBuilder := strings.Builder{}
// 					stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", v_len))

// 					for _, value := range v {
// 						stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
// 					}
// 					conn.Write([]byte(stringBuilder.String()))

// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
// 				}
// 			case "LLEN":
// 				if len(commands) == 2 {
// 					v := safeList.LLen(commands[1])
// 					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
// 				}
// 			case "TYPE":
// 				if len(commands) == 2 {
// 					var typeName string
// 					var ok bool

// 					if typeName, ok = safeMap.Type(commands[1]); ok {
// 					} else if typeName, ok = safeList.Type(commands[1]); ok {
// 					} else if typeName, ok = safeStream.Type(commands[1]); ok {
// 					} else {
// 						typeName = "none"
// 					}
// 					conn.Write([]byte(fmt.Sprintf("+%s\r\n", typeName)))
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
// 				}
// 			case "XADD":
// 				if len(commands) >= 5 {

// 					fields := make(map[string]interface{})

// 					for i := 3; i < len(commands); i += 2 {
// 						if i+1 < len(commands) {
// 							fields[commands[i]] = commands[i+1]
// 						} else {
// 							conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
// 							continue
// 						}
// 					}

// 					id, ok, errStr := safeStream.XAdd(commands[1], commands[2], fields)
// 					if ok {
// 						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
// 					} else {
// 						conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", errStr)))
// 					}
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
// 				}
// 			case "XRANGE":
// 				if len(commands) == 4 {
// 					items, ok := safeStream.XRange(commands[1], commands[2], commands[3])
// 					if ok {
// 						stringBuilder := strings.Builder{}
// 						stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(items)))

// 						for _, item := range items {
// 							stringBuilder.WriteString("*2\r\n")
// 							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Id), item.Id))

// 							// each item has key and value
// 							stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(item.Fields)*2))
// 							for k, v := range item.Fields {
// 								valStr := toRespString(v.Value)
// 								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
// 								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr))
// 							}
// 						}

// 						conn.Write([]byte(stringBuilder.String()))

// 					} else {
// 						conn.Write([]byte("*0\r\n"))
// 					}

// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
// 				}
// 			case "XREAD":

// 				var timeout float64
// 				var streamIndex = 1

// 				if strings.ToLower(commands[1]) == "block" {
// 					if len(commands) < 4 {
// 						conn.Write([]byte("-ERR syntax error\r\n"))
// 						continue
// 					}
// 					// t, err := strconv.Atoi(commands[2])
// 					timeout, err = strconv.ParseFloat(commands[2], 64)
// 					if err != nil || timeout < 0 {
// 						conn.Write([]byte("-ERR invalid timeout\r\n"))
// 						continue
// 					}
// 					streamIndex = 3
// 				} else {
// 					// no block
// 					timeout = -1
// 				}

// 				if len(commands) <= streamIndex || strings.ToLower(commands[streamIndex]) != "streams" {
// 					conn.Write([]byte("-ERR syntax error missing streams\r\n"))
// 					continue
// 				}

// 				streamIndex++
// 				streamCount := (len(commands) - streamIndex) / 2

// 				if (len(commands)-streamIndex)%2 != 0 || streamCount == 0 {
// 					conn.Write([]byte("-ERR wrong number of arguments for 'xread'\r\n"))
// 					continue
// 				}

// 				keys := commands[streamIndex : streamIndex+streamCount]
// 				ids := commands[streamIndex+streamCount:]
// 				if len(keys) != len(ids) {
// 					conn.Write([]byte("-ERR wrong number of key and Ids for 'xread' command\r\n"))
// 					continue
// 				}

// 				// timeout = 0
// 				result := safeStream.XRead(keys, ids, time.Duration(timeout*float64(time.Millisecond)))
// 				if result == nil {
// 					conn.Write([]byte("$-1\r\n"))
// 					continue
// 				}

// 				stringBuilder := strings.Builder{}
// 				stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(result))) // top-level array

// 				for _, key := range keys {
// 					items, ok := result[key]
// 					if !ok {
// 						continue
// 					}
// 					stringBuilder.WriteString("*2\r\n")                                    // key + items
// 					stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)) // key

// 					stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(items))) // entries
// 					for _, item := range items {
// 						stringBuilder.WriteString("*2\r\n") // id + field-value array
// 						stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Id), item.Id))
// 						stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(item.Fields)*2))
// 						for k, v := range item.Fields {
// 							valStr := toRespString(v.Value)
// 							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
// 							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr))
// 						}
// 					}
// 				}
// 				conn.Write([]byte(stringBuilder.String()))
// 			case "INCR":
// 				if len(commands) == 2 {
// 					key := commands[1]
// 					newVal, ok, errStr := safeMap.Incr(key)
// 					if ok {
// 						conn.Write([]byte(fmt.Sprintf(":%d\r\n", newVal)))
// 					} else {
// 						conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", errStr)))
// 					}
// 				} else {
// 					// conn.Write([]byte("-ERR wrong number of arguments for 'incr' command\r\n"))
// 				}
// 			default:
// 				conn.Write([]byte("-ERR unknown command\r\n"))
// 			}

// 			readBulkCommand = false
// 			commands = nil
// 			command_count = 0
// 			isReadFirstByte = false

// 		} else {

// 		}

// 	}
// }
