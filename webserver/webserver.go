package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-redis/redis"
)

var client *redis.Client

type data struct {
	Key string
	Val string
}

func main() {

	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()

	http.HandleFunc("/solved", solvedHandler)
	http.HandleFunc("/unsolved", unsolvedHandler)

	log.Fatal(http.ListenAndServe("localhost:6060", nil))

}

func solvedHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

	// выбираем БД №0 - разложенные числа
	cmd := redis.NewStringCmd("select", 0)
	err := client.Process(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// получаем все ключи из БД
	keys := client.Keys("*")

	var solved []data
	var item data

	// для каждого ключа получаем значение и добавляем в массив
	for _, key := range keys.Val() {
		item.Key = key
		val, err := client.Get(key).Result()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		item.Val = val

		solved = append(solved, item)
	}

	// десериализуем массив в JSON
	err = json.NewEncoder(w).Encode(solved)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}

func unsolvedHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

	cmd := redis.NewStringCmd("select", 1)
	err := client.Process(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	keys := client.Keys("*")

	var solved []data
	var item data

	for _, key := range keys.Val() {
		item.Key = key
		val, err := client.Get(key).Result()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		item.Val = val

		solved = append(solved, item)
	}

	err = json.NewEncoder(w).Encode(solved)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}