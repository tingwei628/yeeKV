package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func buildRESP(args []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return resp
}

func readRESP(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+': // Simple String
		return line[1:], nil
	case '-': // Error
		return "", fmt.Errorf(line[1:])
	case ':': // Integer
		return line[1:], nil
	case '$': // Bulk String
		size, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if size == -1 {
			return "", nil // Null bulk string
		}
		buf := make([]byte, size+2) // +2 for \r\n
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return "", err
		}
		return string(buf[:size]), nil
	case '*': // Array
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if count == -1 {
			return "", nil // Null array
		}

		var builder strings.Builder
		builder.WriteString(fmt.Sprintf("Array of %d elements:\n", count))
		for i := 0; i < count; i++ {
			elem, err := readRESP(reader)
			if err != nil {
				return "", err
			}
			builder.WriteString(fmt.Sprintf("%d) %s\n", i+1, elem))
		}
		return builder.String(), nil

	default:
		return "", fmt.Errorf("unknown response type: %s", line)
	}
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	console := bufio.NewReader(os.Stdin)

	fmt.Println("yeeKV CLI connected. Type commands (type 'exit' to quit).")

	for {
		fmt.Print("yeeKV> ")
		// input, err := console.ReadString('\n')
		input, _ := console.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		// if err != nil {
		// 	fmt.Println("Read input error:", err)
		// 	return
		// }

		if strings.ToLower(input) == "exit" {
			fmt.Println("bye~")
			return
		}
		// if strings.ToLower(input) == "exit" {
		// 	fmt.Println("bye~")
		// 	return
		// }

		args := strings.Fields(input)
		req := buildRESP(args)

		_, err = conn.Write([]byte(req))
		if err != nil {
			fmt.Println("Write to server error:", err)
			// return
			continue
		}

		resp, err := readRESP(reader)
		if err != nil {
			fmt.Println("Read response error:", err)
			// return
			continue
		}

		fmt.Println(resp)
	}
}
