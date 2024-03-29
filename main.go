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

const PLATFORM_CODE = "PLATFORM_CODE"
const NOTIFY_API_URL = "NOTIFY_API_URL"
const PROVIDER_API_URL = "PROVIDER_API_URL"

const AMQP_CONNECTION_STRING = "AMQP_CONNECTION_STRING"
const AMQP_EXCHANGE = "AMQP_EXCHANGE"

const OPERATOR = "OPERATOR"

func readEnv(name string) string {
	logrus.Infof("readEnv name:[%v]", name)
	logrus.Infof("readEnv os.Getenv:[%v]", os.Getenv(name))
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

	_url := readEnv(env)
	if _url == "" {
		return nil
	}
	amqpUrl, err := url.Parse(_url)
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
	span.End()
}

func printGameProvide(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	printJSON(ctx, gameProvide)
}

func printShift(ctx context.Context, shift *pbRecorder.ShiftRecord) {
	printJSON(ctx, shift)
}

func printShoe(ctx context.Context, shoe *pbRecorder.ShoeRecord) {
	printJSON(ctx, shoe)
}

func printRound(ctx context.Context, round *pbRecorder.RoundRecord) {
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

	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			logrus.Error("Error shutting down tracer provider: %v", err)
		}
	}()

	// provider api 服務
	// conn, err := grpc.Dial(readEnvMustNotEmpty(PROVIDER_API_URL))
	// if err != nil {
	// 	log.Panic(err)
	// }
	// record := pbRecorder.NewRecorderReadServiceClient(conn)
	// provider := pbRecorder.NewProviderServiceClient(conn)

	// self host queue
	service := readEnvMustNotEmpty(OPERATOR)
	platformCode := readEnvMustNotEmpty(PLATFORM_CODE)
	exchange := readEnvMustNotEmpty(AMQP_EXCHANGE)
	selfHostAmqp := loadAMQPClient(AMQP_CONNECTION_STRING)

	wecasinoQueue := queue.NewCasinoQueue(service, platformCode, exchange, selfHostAmqp)

	wecasinoQueue.HandleGameProvideStateChange(printGameProvide)
	wecasinoQueue.HandleDealerLogin(printGameProvide)
	wecasinoQueue.HandleDealerLogout(printGameProvide)
	wecasinoQueue.HandleGameChangingShoe(printGameProvide)

	wecasinoQueue.HandleShiftStart(printShift)
	wecasinoQueue.HandleShiftEnd(printShift)

	wecasinoQueue.HandleShoeStart(printShoe)
	wecasinoQueue.HandleShoeEnd(printShoe)

	wecasinoQueue.HandleRoundStart(printRound)
	wecasinoQueue.HandleRoundBet(printRound)
	wecasinoQueue.HandleRoundNoMoreBet(printRound)
	wecasinoQueue.HandleRoundStep(printRound)
	wecasinoQueue.HandleRoundFinish(printRound)
	wecasinoQueue.HandleRoundCancel(printRound)

	// notify api
	notifyApi := loadAMQPClient(NOTIFY_API_URL)
	notifyApi.QueueDeclare(weamqp.QueueDeclare{
		Name:       platformCode,
		AutoDelete: false,
	})
	notifyApi.SubscribeQueue(platformCode, false, wecasinoQueue.GenInputFunc())

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
