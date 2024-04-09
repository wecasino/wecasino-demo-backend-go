package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
)

const AMQP_CONNECTION_STRING = "AMQP_CONNECTION_STRING"
const AMQP_QUEUE = "AMQP_QUEUE"

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file error:[%v]", err)
	}

	viper.SetDefault("amqp_url", os.Getenv(AMQP_CONNECTION_STRING))
	viper.SetDefault("amqp_queue", os.Getenv(AMQP_QUEUE))

	log.Println("[Config] AMQP_CONNECTION_STRING:" + viper.GetString("amqp_url"))
	log.Println("[Config] AMQP_QUEUE:" + viper.GetString("amqp_queue"))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// 连接到RabbitMQ服务器
	amqpUrl := viper.GetString("amqp_url")
	conn, err := amqp.Dial(amqpUrl)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 打开一个通道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	queueName := viper.GetString("amqp_queue")
	// queueName := "yx-uat"
	// 声明一个队列
	q, err := ch.QueueDeclare(
		queueName, // queue name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 注册一个消费者来接收消息
	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for delivery := range msgs {
			// log.Printf("Received a message: %s", d.Body)

			amqpCallback(delivery)

			err = delivery.Ack(false)
			if err != nil {
				logrus.Fatalf("failed ack: %v", err)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func amqpCallback(delivery amqp.Delivery) {
	notifyType := pbRecorder.GameNotifyType(pbRecorder.GameNotifyType_value[delivery.Type])

	logrus.Infof("amqpCallback notiftyType:[%v]", notifyType)
}
