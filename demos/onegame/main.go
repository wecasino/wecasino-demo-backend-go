package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
	"github.com/wecasino/wecasino-example-backend-go/weamqp"
	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
	"go.opentelemetry.io/otel"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/proto"

	"github.com/wecasino/wecasino-example-backend-go/demos/onegame/hook"
)

const PLATFORM_CODE = "PLATFORM_CODE"
const NOTIFY_API_URL = "NOTIFY_API_URL"
const PROVIDER_API_URL = "PROVIDER_API_URL"
const GAME_CODE = "GAME_CODE"

var testGame = ""

func initTracer() func() {
	exporter, err := stdout.New()
	if err != nil {
		log.Fatalf("failed to initialize stdout exporter: %v", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tracerProvider)

	return func() {
		err := tracerProvider.Shutdown(context.Background())
		if err != nil {
			log.Fatalf("failed to shutdown TracerProvider: %v", err)
		}
	}
}

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

func getTraceId(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return uuid.NewString()
	}
}

func handleMessage(msg amqp091.Delivery) {

	log.Info("==================")
	tracer := otel.Tracer("AMQPCallBack")
	traceId := getTraceId(msg.Headers["traceID"])
	traceCtx, span := tracer.Start(context.Background(), traceId)

	log.AddHook(hook.NewTraceIdHook(span.SpanContext().TraceID().String(),
		span.SpanContext().SpanID().String(), span))

	log.Printf("[AMQPCallBack] header:[%v]", msg.Headers)

	notifyType := pbRecorder.GameNotifyType(pbRecorder.GameNotifyType_value[msg.Type])
	log.Printf("[AMQPCallBack] notifyType:[%v]", notifyType)

	typeUrl := msg.Type
	gameCode := fmt.Sprintf("%v", msg.Headers["gameCode"])
	if strings.Contains(gameCode, testGame) {

		log.WithContext(traceCtx).Infof("[AMQPCallBack] receive gameCode:[%v]", gameCode)

		switch typeUrl {
		case pbRecorder.GameNotifyType_GAME_NOTIFY_TYPE_UNSPECIFIED.String():
			record := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_GAME_NOTIFY_TYPE_UNSPECIFIED GameProvide:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String():
			record := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE GameProvide:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGIN.String():
			record := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_GAME_DEALER_LOGIN GameProvide:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT.String():
			record := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT GameProvide:[%#v]", record)

			//對應紅卡換靴
		case pbRecorder.GameNotifyType_NOTIFY_GAME_CHANGING_SHOE.String():
			record := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_GAME_CHANGING_SHOE GameProvide:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_SHIFT_START.String():
			record := &pbRecorder.ShiftRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_SHIFT_START ShiftRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_SHIFT_END.String():
			record := &pbRecorder.ShiftRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_SHIFT_END ShiftRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_SHOE_START.String():
			record := &pbRecorder.ShoeRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_SHOE_START ShoeRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_SHOE_END.String():
			record := &pbRecorder.ShoeRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_SHOE_END ShoeRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_START.String():
			round := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, round)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_START RoundRecord:[%#v]", round)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_BET.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_BET RoundRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_NO_MORE_BET.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_NO_MORE_BET RoundRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_STEP.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_STEP RoundRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_FINISH.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_FINISH RoundRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_CANCEL.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_CANCEL RoundRecord:[%#v]", record)

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_PLAYBACK.String():
			record := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(msg.Body, record)
			if err != nil {
				log.Errorf("[AMQPCallBack] proto unmarshal error:[%v]", err)
			}
			// log.Infof("[AMQPCallBack] GameNotifyType_NOTIFY_ROUND_PLAYBACK RoundRecord:[%#v]", record)
		}
	}
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file error:[%v]", err)
	}
}

func main() {

	defer initTracer()()

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

	testGame = readEnvMustNotEmpty(GAME_CODE)

	notifyApi.Connect()

	// 監聽關機訊號
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shut down start")
	notifyApi.Close()
	log.Println("shut down complete")
}