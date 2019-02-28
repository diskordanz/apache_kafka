/*
	процессор данных, сгенерированных генератором
	программа получает числа из очереди "Generated" kafka
	и факторизует их
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	redis "github.com/go-redis/redis"
	kafka "github.com/segmentio/kafka-go"
)
 
const inputTopic = "Generated"

type data struct {
	Number  int
	Factors []int
}

var client *redis.Client

func main() {

	// подписчик очереди Kafka (consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   inputTopic,
		// GroupID:   "consumer-group-id-3",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	// клиент БД Redis
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer client.Close()

	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println("1")
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	c := 0 //counter

	for {

		// создайм объект контекста с таймаутом в 15 секунд для чтения сообщений
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// читаем очередное сообщение из очереди
		// поскольку вызов блокирующий - передаём контекст с таймаутом
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("3")
			fmt.Println(err)
			break
		}

		wg.Add(1)
		// создайм объект контекста с таймаутом в 10 миллисекунд для каждой вычислительной горутины
		goCtx, goCcancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer goCcancel()

		// вызываем функцию обработки сообщения (факторизации)
		go process(goCtx, c, &wg, m)
		c++
	}

	// ожидаем завершения всех горутин
	wg.Wait()

	err = r.Close()
	if err != nil {
		fmt.Println("4")
		log.Fatal(err)
	}
}

func process(ctx context.Context, counter int, wg *sync.WaitGroup, m kafka.Message) (factors []int) {
	defer wg.Done()

	n, err := strconv.Atoi(string(m.Value))
	if err != nil {
		fmt.Println("5")
		log.Fatal(err)
	}

	outChan := make(chan []int)

	// собственно факторизация
	go factorize(n, outChan)

	var item data

	select {
	case factors = <-outChan:
		{
			fmt.Printf("\ngoroutine #%d, input: %d, factors: %+v\n", counter, n, factors)
			item.Number = n
			item.Factors = factors
			err = storeSolved(item)
			if err != nil {
				fmt.Println("6")
				log.Fatal(err)
			}
		}
	case <-ctx.Done():
		{
			fmt.Printf("\ngoroutine #%d, input: %d, exited on context timeout\n", counter, n)
			err = storeUnsolved(n)
			if err != nil {
				fmt.Println("7")
				log.Fatal(err)
			}
			return nil
		}
	}

	return factors
}

func factorize(n int, out chan<- []int) {

	factors := []int{1}

	max := int(math.Trunc(math.Sqrt(float64(n))))

	if n%2 == 0 {
		factors = append(factors, 2)
	}
	if n%3 == 0 {
		factors = append(factors, 3)
	}

	for i := 5; i < max; i++ {
		if (n%i) == 0 && isPrime(i) {
			factors = append(factors, i)
		}
	}

	// если исходное число простое и в множителях только '1', то добавляем себя же
	if len(factors) == 1 {
		factors = append(factors, n)
	}

	out <- factors
}

func isPrime(n int) bool {
	for i := 2; i < n-1; i++ {
		if n%i == 0 {
			return false
		}
	}

	return true
}

func storeSolved(item data) (err error) {
	// переключаемся на БД 0
	cmd := redis.NewStringCmd("select", 0)
	err = client.Process(cmd)
	b, err := json.Marshal(item.Factors)
	err = client.Set(strconv.Itoa(item.Number), string(b), 0).Err()
	return err
}

func storeUnsolved(n int) (err error) {
	// переключаемся на БД 1
	cmd := redis.NewStringCmd("select", 1)
	err = client.Process(cmd)
	err = client.Set(strconv.Itoa(n), strconv.Itoa(n), 0).Err()
	return err

}
