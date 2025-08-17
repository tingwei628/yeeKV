package server

import (
	"bufio"
	"net"
	"strconv"
	"strings"
)

func parseCommands(conn net.Conn) ([]string, error) {

	scanner := bufio.NewScanner(conn)

	var commands []string
	var readBulkCommand bool
	var isReadFirstByte bool
	var commandCount int

	for scanner.Scan() {
		text := scanner.Text()

		// Handle the command
		text = strings.TrimSpace(text)
		// fmt.Println(text)

		if strings.HasPrefix(text, "*") && !isReadFirstByte {

			count, err := strconv.Atoi(text[1:])
			if err != nil {
				return nil, err
			}
			commandCount = count
			commands = make([]string, 0, commandCount)
			isReadFirstByte = true
			continue
		}

		if strings.HasPrefix(text, "$") {
			readBulkCommand = true
			// if len(text) > 1 {
			// 	continue
			// }
			continue
		}

		if readBulkCommand && isReadFirstByte {
			commands = append(commands, text)
			readBulkCommand = false
		}

		if len(commands) == commandCount {
			return commands, nil
		}

	}

	return nil, scanner.Err()
}
