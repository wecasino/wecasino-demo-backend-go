package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	// "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sirupsen/logrus"
	"github.com/wecasino/wecasino-example-backend-go/weamqp"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
	"google.golang.org/protobuf/proto"
)

type GameProvideHandler func(context.Context, *pbRecorder.GameProvide)
type GameShiftHandler func(context.Context, *pbRecorder.ShiftRecord)
type GameShoeHandler func(context.Context, *pbRecorder.ShoeRecord)
type GameRoundHandler func(context.Context, *pbRecorder.RoundRecord)

const KeyErrorHandler = "KEY_ERROR_HANDLER"

type WECasinoQueue struct {
	instanceId   string
	service      string
	platformCode string
	exchange     string

	tracer trace.Tracer
	amqp   *weamqp.Client

	handlers sync.Map
}

func (s *WECasinoQueue) HandleGameProvideStateChange(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String(), handler)
}

func (s *WECasinoQueue) HandleDealerLogin(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGIN.String(), handler)
}

func (s *WECasinoQueue) HandleDealerLogout(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT.String(), handler)
}

func (s *WECasinoQueue) HandleGameChangingShoe(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_CHANGING_SHOE.String(), handler)
}

func (s *WECasinoQueue) HandleShiftStart(handler GameShiftHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHIFT_START.String(), handler)
}

func (s *WECasinoQueue) HandleShiftEnd(handler GameShiftHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHIFT_END.String(), handler)
}

func (s *WECasinoQueue) HandleShoeStart(handler GameShoeHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHOE_START.String(), handler)
}

func (s *WECasinoQueue) HandleShoeEnd(handler GameShoeHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHOE_END.String(), handler)
}

func (s *WECasinoQueue) HandleRoundStart(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_START.String(), handler)
}

func (s *WECasinoQueue) HandleRoundBet(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_BET.String(), handler)
}

func (s *WECasinoQueue) HandleRoundNoMoreBet(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_NO_MORE_BET.String(), handler)
}

func (s *WECasinoQueue) HandleRoundStep(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_STEP.String(), handler)
}

func (s *WECasinoQueue) HandleRoundFinish(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_FINISH.String(), handler)
}

func (s *WECasinoQueue) HandleRoundCancel(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_CANCEL.String(), handler)
}

func (s *WECasinoQueue) HandleRoundPlayback(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_PLAYBACK.String(), handler)
}

func getTraceId(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return uuid.NewString()
	}
}

func logError(ctx context.Context, err error, msg ...any) {
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, fmt.Sprint(msg...))
	span.RecordError(err)
	span.End()
}

var ErrGameCode = errors.New("gameCode not equal with header")

func (s *WECasinoQueue) genGameHandler(gameCode string) func(amqp091.Delivery) {
	return func(delivery amqp091.Delivery) {

		traceId := getTraceId(delivery.Headers["traceID"])
		ctx, _ := s.tracer.Start(context.Background(), traceId)

		// span := trace.SpanFromContext(ctx)
		// for key, value := range delivery.Headers {
		// 	span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", value)))
		// }
		// defer span.End()

		notifyType := pbRecorder.GameNotifyType(pbRecorder.GameNotifyType_value[delivery.Type])

		logrus.Infof("receive game notifyType:[%v]", notifyType)
		switch notifyType {

		case pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGIN,
			pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT,
			pbRecorder.GameNotifyType_NOTIFY_GAME_CHANGING_SHOE:

			gameProvide := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(delivery.Body, gameProvide)
			if err != nil {
				logError(ctx, err)
				return
			}
			if gameProvide.GameCode != gameCode {
				logError(ctx, ErrGameCode)
				return
			}
			if value, ok := s.handlers.Load(notifyType.String()); ok {
				if handle, ok := value.(GameProvideHandler); ok {
					handle(ctx, gameProvide)
				}
			}

		case pbRecorder.GameNotifyType_NOTIFY_SHIFT_START,
			pbRecorder.GameNotifyType_NOTIFY_SHIFT_END:
			shift := &pbRecorder.ShiftRecord{}
			err := proto.Unmarshal(delivery.Body, shift)
			if err != nil {
				logError(ctx, err)
			}
			if gameCode != "" && shift.GameCode != gameCode {
				logError(ctx, ErrGameCode)
			}
			if value, ok := s.handlers.Load(notifyType.String()); ok {
				if handle, ok := value.(GameShiftHandler); ok {
					handle(ctx, shift)
				}
			}

		case pbRecorder.GameNotifyType_NOTIFY_SHOE_START,
			pbRecorder.GameNotifyType_NOTIFY_SHOE_END:
			shoe := &pbRecorder.ShoeRecord{}
			err := proto.Unmarshal(delivery.Body, shoe)
			if err != nil {
				logError(ctx, err)
			}
			if gameCode != "" && shoe.GameCode != gameCode {
				logError(ctx, ErrGameCode)
			}
			if value, ok := s.handlers.Load(notifyType.String()); ok {
				if handle, ok := value.(GameShoeHandler); ok {
					handle(ctx, shoe)
				}
			}

		case pbRecorder.GameNotifyType_NOTIFY_ROUND_START,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_BET,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_NO_MORE_BET,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_STEP,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_FINISH,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_CANCEL,
			pbRecorder.GameNotifyType_NOTIFY_ROUND_PLAYBACK:
			round := &pbRecorder.RoundRecord{}
			err := proto.Unmarshal(delivery.Body, round)
			if err != nil {
				logError(ctx, err)
			}
			if gameCode != "" && round.GameCode != gameCode {
				logError(ctx, ErrGameCode)
			}
			if value, ok := s.handlers.Load(notifyType.String()); ok {
				if handle, ok := value.(GameRoundHandler); ok {
					handle(ctx, round)
				}
			}

		}
	}
}

func (s *WECasinoQueue) genProvideStateChangeHandler() func(amqp091.Delivery) {
	return func(delivery amqp091.Delivery) {
		log.Printf("receive StateChange headers: %v", delivery.Headers)

		traceId := getTraceId(delivery.Headers["traceID"])
		ctx, _ := s.tracer.Start(context.Background(), traceId)

		if delivery.Type != pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String() {
			return
		}

		gameProvide := pbRecorder.GameProvide{}
		err := proto.Unmarshal(delivery.Body, &gameProvide)
		if err != nil {
			log.Printf("receive message but proto unmarshal fail with err: %v", err)
			return
		}
		gameCode := strings.TrimSpace(gameProvide.GameCode)
		if gameCode == "" {
			log.Print("gameCode empty")
			return
		}
		logrus.WithContext(ctx).Printf("receive StateChange gameProvide.State:[%v]", gameProvide.State)

		queue := fmt.Sprintf("%v:%v:game:%v", s.service, s.platformCode, gameProvide.GameCode)

		switch gameProvide.State {
		case pbRecorder.GameProvideState_GAME_PROVIDE_AVAILABLE, pbRecorder.GameProvideState_GAME_PROVIDE_CLOSE_AFTER_ROUND, pbRecorder.GameProvideState_GAME_PROVIDE_MAINTEN_AFTER_ROUND:

			if s.amqp == nil {
				return
			}

			s.amqp.QueueDeclare(weamqp.QueueDeclare{
				Name:       queue,
				AutoDelete: false, // 手動檢查刪除
				Durable:    true,
			})
			s.amqp.QueueBindDeclare(weamqp.QueueBindDeclare{
				Exchange: s.exchange,
				Queue:    queue,
				Headers: amqp091.Table{
					"x-match":      "all",
					s.platformCode: true,
					"gameCode":     gameCode,
				},
			})
			// s.amqp.SubscribeQueue(ctx, queue, false, s.genGameHandler(gameCode))
		default:
			logrus.Infof("enter default gameProvide.State:[%v]", gameProvide.State)
		}
	}
}

func (s *WECasinoQueue) GenInputFunc() func(amqp091.Delivery) {
	return func(d amqp091.Delivery) {
		if s.amqp == nil {
			return
		}
		// logrus.Infof("GenInputFunc exchange:[%v]", s.exchange)
		// logrus.Infof("GenInputFunc headers:[%v]", d.Headers)
		s.amqp.Publish(context.Background(), s.exchange, d.RoutingKey, &amqp091.Publishing{
			Headers:         d.Headers,
			ContentType:     d.ContentType,
			ContentEncoding: d.ContentEncoding,
			DeliveryMode:    d.DeliveryMode,
			Priority:        d.Priority,
			CorrelationId:   d.CorrelationId,
			ReplyTo:         d.ReplyTo,
			Expiration:      d.Expiration,
			MessageId:       d.MessageId,
			Timestamp:       d.Timestamp,
			Type:            d.Type,
			UserId:          d.UserId,
			AppId:           d.AppId,
			Body:            d.Body,
		})
	}
}

func (s *WECasinoQueue) Start() {
	if s.amqp == nil {
		logrus.Infof("[AMQP] Start amqp is nil")
		return
	}
	logrus.Infof("[AMQP] Start then Connect")
	s.amqp.Connect()

}

func (s *WECasinoQueue) End() {
	if s.amqp == nil {
		return
	}

	s.amqp.Close()
}

func NewCasinoQueue(ctx context.Context, service, platformCode, exchange string, amqp *weamqp.Client) *WECasinoQueue {

	instanceId := uuid.NewString()
	queue := fmt.Sprintf("%v:%v:provide:%v", service, platformCode, instanceId)
	logrus.Infof("NewCasinoQueue queue:[%v]", queue)

	amqp.ExchangeDeclare(weamqp.ExchangeDeclare{
		Name:    exchange,
		Kind:    weamqp.ExchangeHeaders,
		Durable: true,
	})

	amqp.QueueDeclare(weamqp.QueueDeclare{
		Name: queue,
		Durable:   true,
		Exclusive: true,
	})

	amqp.QueueBindDeclare(weamqp.QueueBindDeclare{
		Exchange: exchange,
		Queue:    queue,
		Headers: amqp091.Table{
			"x-match":    "all",
			"notifyType": pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String(),
			platformCode: true,
		},
	})

	s := &WECasinoQueue{
		instanceId:   instanceId,
		service:      service,
		platformCode: platformCode,
		exchange:     exchange,
		tracer:       otel.GetTracerProvider().Tracer(service),
		amqp:         amqp,
		handlers:     sync.Map{},
	}
	amqp.SubscribeQueue(ctx, queue, true, s.genProvideStateChangeHandler())

	return s
}

func ReceiveGameExchangeQueue(ctx context.Context, platformCode, exchange string, amqp *weamqp.Client, fn func(amqp091.Delivery)) {
	amqp.ExchangeDeclare(weamqp.ExchangeDeclare{
		Name:    exchange,
		Kind:    weamqp.ExchangeHeaders,
		Durable: true,
	})
	amqp.QueueDeclare(weamqp.QueueDeclare{
		Name:       platformCode,
		AutoDelete: false,
	})
	amqp.QueueBindDeclare(weamqp.QueueBindDeclare{
		Exchange: exchange,
		Queue:    platformCode,
		Headers: amqp091.Table{
			"x-match":    "all",
			platformCode: true,
		},
	})

	amqp.SubscribeQueue(ctx, platformCode, false, fn)
}
