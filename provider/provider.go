package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Car struct {
	Number         string `json:"number"`
	Model          string `json:"model"`
	Year           int    `json:"year"`
	Mileage        int    `json:"mileage"`
	InspectionDate string `json:"inspection_date"`
	Color          string `json:"color"`
}

func main() {

	file := flag.String("file", "provider/cars.json", "a string")
	topic := flag.String("topic", "topic1", "a string")
	port := flag.String("port", "localhost:9092", "a string")

	flag.Parse()

	jsonFile, err := os.Open(*file)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(*file + "successfully opened ")
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var cars []Car
	json.Unmarshal(byteValue, &cars)

	// клиент очереди
	conn, err := kafka.DialLeader(context.Background(), "tcp", *port, *topic, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for _, k := range cars {

		if err = conn.SetWriteDeadline(time.Now().Add(1 * time.Second)); err != nil {
			log.Fatal(err)
		}

		// пишем в очередь
		m, _ := json.Marshal(k)
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(m)},
		)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Written to %v: %v", *topic, k)

	}
}
