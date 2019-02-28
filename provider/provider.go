package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
	"flag"
	"os"
	"encoding/json"
	"io/ioutil"
	kafka "github.com/segmentio/kafka-go"
	model "github.com/diskordanz/apache_kafka/model"
)
 
const topic = "Provided"

func main() {

	file := flag.String("filename", "cars.json" , "a string")

	flag.Parse()

	jsonFile, err := os.Open(*file)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("successfully opened" + *file)
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var cars model.Cars
	json.Unmarshal(byteValue, &cars)

	for i := 0; i < len(cars.Cars); i++ {
		fmt.Println("Number car: " + cars.Cars[i].Number)
		fmt.Println("Model car: " + cars.Cars[i].Model)
		fmt.Println("Yaer car: " + strconv.Itoa(cars.Cars[i].Year))
		fmt.Println("Mileage car: " + strconv.Itoa(cars.Cars[i].Mileage))
		fmt.Println("Inspection date car: " + cars.Cars[i].InspectionDate)
		fmt.Println("Color car: " + cars.Cars[i].Color)
	}

	// клиент очереди
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	

	for _, k := range cars.Cars{

		err = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			log.Fatal(err)
		}

		// пишем в очередь
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(json.Marshal(k))},
		)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Written to topic:", n)

		time.Sleep(time.Millisecond * time.Duration(rand.Intn(4000)))

	}

}