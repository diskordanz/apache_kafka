package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

type data struct {
	Number         string `json:"number"`
	Model          string `json:"model"`
	Year           int    `json:"year"`
	Mileage        int    `json:"mileage"`
	InspectionDate string `json:"inspection_date"`
	Color          string `json:"color"`
}

func main() {

	topic := flag.String("topic", "topic1", "a string")
	port := flag.String("port", "localhost:9092", "a string")

	flag.Parse()

	// подписчик очереди Kafka (consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*port},
		Topic:     *topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

	}

	err := r.Close()
	if err != nil {
		log.Fatal(err)
	}
}
