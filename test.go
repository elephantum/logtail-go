package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
)

// import (
// 	"github.com/marpaia/graphite-golang"
// )

// LogData - вертикальное хранение всех данных из лога
type LogData struct {
	Size int

	Request      []string `logfield:"request"`
	StatusRaw    []string `logfield:"status"`
	UpstreamAddr []string `logfield:"upstream_addr"`

	Status   []int    `logresize:"true"`
	StatusXX []string `logresize:"true"`
	Service  []string `logresize:"true"`
}

const (
	filename = "sample.log"
)

func readLines(filename string) [][]byte {
	var res [][]byte

	f, _ := os.Open(filename)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		bb := scanner.Bytes()
		newBb := make([]byte, len(bb))
		copy(newBb, bb)
		res = append(res, newBb)
	}

	return res
}

// Магия, через reflect и таги "logfield"/"logresize" выставить все поля структуры LogData в
// соответствующие значения из json-лога
func parseJSONLines(lines [][]byte, res interface{}) {
	resType := reflect.TypeOf(res).Elem()
	resValue := reflect.ValueOf(res).Elem()

	for i := 0; i < resType.NumField(); i++ {
		field := resValue.Field(i)
		fieldType := resType.Field(i)
		fieldTag := fieldType.Tag
		if fieldTag.Get("logfield") != "" || fieldTag.Get("logresize") != "" {
			field.Set(reflect.MakeSlice(fieldType.Type, len(lines), len(lines)))
		}
	}

	resValue.FieldByName("Size").Set(reflect.ValueOf(len(lines)))

	for lineno, l := range lines {
		var ll map[string]string
		err := json.Unmarshal(l, &ll)
		if err == nil {
			for i := 0; i < resType.NumField(); i++ {
				field := resValue.Field(i)
				fieldTag := resType.Field(i).Tag.Get("logfield")

				if fieldTag != "" {
					valueFromJSON := reflect.ValueOf(ll[fieldTag])
					field.Index(lineno).Set(valueFromJSON)
				}
			}
		} else {
			fmt.Printf("err: %v; %s\n", err, l)
		}
	}
}

func countGroups(input []string) map[string]int {
	res := make(map[string]int)

	for _, i := range input {
		res[i]++
	}

	return res
}

func main() {
	rawLines := readLines(filename)

	var logData LogData
	parseJSONLines(rawLines, &logData)

	for i := 0; i < logData.Size; i++ {
		var status int
		status, _ = strconv.Atoi(logData.StatusRaw[i])
		logData.Status[i] = status

		var service string
		upstreamAddr := logData.UpstreamAddr[i]
		if upstreamAddr == "-" {
			service = "static"
		} else if upstreamAddr == "127.0.0.1:4091" {
			service = "backend"
		} else if upstreamAddr == "127.0.0.1:4092" {
			service = "screenshoter"
		} else if upstreamAddr == "127.0.0.1:9000" {
			service = "frontend"
		}
		logData.Service[i] = service

		var statusXX string
		if status >= 100 && status < 200 {
			statusXX = "1xx"
		} else if status >= 200 && status < 300 {
			statusXX = "2xx"
		} else if status >= 300 && status < 400 {
			statusXX = "3xx"
		} else if status >= 400 && status < 500 {
			statusXX = "4xx"
		} else if status >= 500 && status < 600 {
			statusXX = "5xx"
		} else {
			statusXX = "-"
		}
		logData.StatusXX[i] = statusXX
	}

	fmt.Println(countGroups(logData.Service))
	fmt.Println(countGroups(logData.StatusXX))
}
