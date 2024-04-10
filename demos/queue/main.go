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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/wecasino/wecasino-example-backend-go/queue"
	"github.com/wecasino/wecasino-example-backend-go/weamqp"

	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
)

const SERVICE = "SERVICE"

const PLATFORM_CODE = "PLATFORM_CODE"

const AMQP_CONNECTION_STRING = "AMQP_CONNECTION_STRING"
const AMQP_EXCHANGE = "AMQP_EXCHANGE"
const AMQP_EXCHANGE_DURABLE = "AMQP_EXCHANGE_DURABLE"

const NOTIFY_API_URL = "NOTIFY_API_URL"

var countRound = 0
var countRoundStart = 0
var countRoundBet = 0
var countRoundNoMoreBet = 0
var countRoundFinish = 0

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

func loadAMQPClient(env string) *weamqp.Client {

	connectString := readEnvMustNotEmpty(env)
	amqpUrl, err := url.Parse(connectString)
	if err != nil {
		log.Fatalf("amqp url: %v parse failed with error: %v", amqpUrl, err)
	}
	client := weamqp.NewClient(*amqpUrl, nil)
	return client
}

func printJSON(ctx context.Context, object any) {
	span := trace.SpanFromContext(ctx)
	data, err := json.Marshal(object)
	if err != nil {
		span.SetStatus(codes.Error, "json marshal fail")
		span.RecordError(err)
	} else if len(data) > 0 {
		span.SetAttributes(attribute.String("json", string(data)))
	}
	// defer span.End()
}

func HandleGameProvideStateChange(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	printJSON(ctx, gameProvide)
}

func HandleDealerLogin(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	printJSON(ctx, gameProvide)
}

func HandleDealerLogout(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	printJSON(ctx, gameProvide)
}

func HandleGameChangingShoe(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	printJSON(ctx, gameProvide)
}

func HandleShiftStart(ctx context.Context, shift *pbRecorder.ShiftRecord) {
	printJSON(ctx, shift)
}

func HandleShiftEnd(ctx context.Context, shift *pbRecorder.ShiftRecord) {
	printJSON(ctx, shift)
}

func HandleShoeStart(ctx context.Context, shoe *pbRecorder.ShoeRecord) {
	printJSON(ctx, shoe)
}

func HandleShoeEnd(ctx context.Context, shoe *pbRecorder.ShoeRecord) {
	printJSON(ctx, shoe)
}

func HandleRoundStart(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.Infof("HandleRoundStart")
	printJSON(ctx, round)
	countRoundStart++
}

func HandleRoundBet(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.Infof("HandleRoundBet")
	printJSON(ctx, round)
	countRoundBet++
}

func HandleRoundNoMoreBet(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.Infof("HandleRoundNoMoreBet")
	printJSON(ctx, round)
	countRoundNoMoreBet++
}

func HandleRoundStep(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.Infof("HandleRoundStep")
	printJSON(ctx, round)
}

func HandleRoundFinish(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.Infof("HandleRoundFinish")
	countRoundFinish++
	countRound++
	logrus.Infof("Round:[%v]", countRound)
	logrus.Infof("RoundStart:[%v], RoundBet:[%v], RoundNoMoreBet:[%v], RoundFinish:[%v]",
		countRoundStart, countRoundBet, countRoundNoMoreBet, countRoundFinish)
	logrus.Infof("=======================")
	printJSON(ctx, round)

}

func HandleRoundCancel(ctx context.Context, round *pbRecorder.RoundRecord) {
	printJSON(ctx, round)
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file error:[%v]", err)
	}
}

func main() {

	ctx := context.Background()

	// log
	exporter, err := stdout.New()
	if err != nil {
		log.Fatalf("failed to initialize exporter: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)

	// self host queue
	service := readEnvMustNotEmpty(SERVICE)
	platformCode := readEnvMustNotEmpty(PLATFORM_CODE)
	exchange_durable := readEnvMustNotEmpty(AMQP_EXCHANGE_DURABLE)
	selfHostAmqp := loadAMQPClient(AMQP_CONNECTION_STRING)

	wecasinoQueue := queue.NewCasinoQueue(ctx, service, platformCode, exchange_durable, selfHostAmqp)

	wecasinoQueue.HandleGameProvideStateChange(HandleGameProvideStateChange)
	wecasinoQueue.HandleDealerLogin(HandleDealerLogin)
	wecasinoQueue.HandleDealerLogout(HandleDealerLogout)
	wecasinoQueue.HandleGameChangingShoe(HandleGameChangingShoe)

	wecasinoQueue.HandleShiftStart(HandleShiftStart)
	wecasinoQueue.HandleShiftEnd(HandleShiftEnd)

	wecasinoQueue.HandleShoeStart(HandleShoeStart)
	wecasinoQueue.HandleShoeEnd(HandleShoeEnd)

	wecasinoQueue.HandleRoundStart(HandleRoundStart)
	wecasinoQueue.HandleRoundBet(HandleRoundBet)
	wecasinoQueue.HandleRoundNoMoreBet(HandleRoundNoMoreBet)
	wecasinoQueue.HandleRoundStep(HandleRoundStep)
	wecasinoQueue.HandleRoundFinish(HandleRoundFinish)
	wecasinoQueue.HandleRoundCancel(HandleRoundCancel)

	// notify api
	notifyApi := loadAMQPClient(NOTIFY_API_URL)
	exchange := readEnvMustNotEmpty(AMQP_EXCHANGE)
	// notifyApi.ExchangeDeclare(weamqp.ExchangeDeclare{
	// 	Name: exchange,
	// 	Kind: weamqp.ExchangeHeaders,
	// })
	// notifyApi.QueueDeclare(weamqp.QueueDeclare{
	// 	Name:       platformCode,
	// 	AutoDelete: false,
	// })
	// notifyApi.QueueBindDeclare(weamqp.QueueBindDeclare{
	// 	Exchange: exchange,
	// 	Queue:    platformCode,
	// 	Headers: amqp091.Table{
	// 		"platform":   platformCode,
	// 		"x-match":    "any",
	// 		platformCode: true,
	// 	},
	// })
	// notifyApi.SubscribeQueue(ctx, platformCode, false, wecasinoQueue.GenInputFunc())
	queue.ReceiveGameExchangeQueue(ctx, platformCode, exchange, notifyApi, wecasinoQueue.GenInputFunc())

	// start
	wecasinoQueue.Start()
	notifyApi.Connect()

	// 監聽關機訊號
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shut down start")

	notifyApi.Close()
	wecasinoQueue.End()
	if err := tp.Shutdown(ctx); err != nil {
		log.Fatalf("Error shutting down tracer provide: %v", err)
	}
	log.Println("shut down complete")

}
