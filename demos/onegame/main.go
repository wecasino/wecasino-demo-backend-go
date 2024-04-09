package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wecasino/wecasino-example-backend-go/weamqp"
)

const PLATFORM_CODE = "PLATFORM_CODE"
const NOTIFY_API_URL = "NOTIFY_API_URL"
const PROVIDER_API_URL = "PROVIDER_API_URL"

func readEnv(name string) string {
	env := strings.TrimSpace(os.Getenv(name))
	log.Printf("[ENV] load env: %v => [%v]", name, env)
	return env
}

func readEnvMustNotEmpty(name string) string {
	env := readEnv(name)
	if env == "" {
		log.Panicf("[ENV] fail to load env: %v", name)
	}
	return env
}

type DeliveryJson struct {
	Delivery   amqp091.Delivery `json:"delivery"`
	JsonBody   string           `json:"jsonBody"`
	ParseError string           `json:"parseError"`
}

func handleMessage(delivery amqp091.Delivery) {

	body := delivery.Body
	delivery.Body = nil

	data, err := json.Marshal(delivery)
	if err != nil {
		println("[Body]: ", err.Error())
	} else {
		println("[Body]: ", string(data))
	}
	print(body)
}

func main() {

	platformCode := readEnvMustNotEmpty(PLATFORM_CODE)

	amqpUrl, err := url.Parse(readEnvMustNotEmpty(NOTIFY_API_URL))
	if err != nil {
		log.Fatalf("amqp url: %v parse failed with error: %v", amqpUrl, err)
	}
	notifyApi := weamqp.NewClient(*amqpUrl, nil)
	notifyApi.QueueDeclare(weamqp.QueueDeclare{
		Name: platformCode,
	})
	notifyApi.SubscribeQueue(context.Background(), platformCode, true, handleMessage)

	// 監聽關機訊號
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shut down start")
	notifyApi.Close()
	log.Println("shut down complete")
}
