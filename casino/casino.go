package casino

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/wecasino/wecasino-example-backend-go/casino/hook"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
	"google.golang.org/protobuf/proto"
)

type GameProvideHandler func(context.Context, *pbRecorder.GameProvide)
type GameShiftHandler func(context.Context, *pbRecorder.ShiftRecord)
type GameShoeHandler func(context.Context, *pbRecorder.ShoeRecord)
type GameRoundHandler func(context.Context, *pbRecorder.RoundRecord)

type CasinoService struct {
	tracer trace.Tracer

	service  string
	platform string

	provider pbRecorder.ProviderServiceClient
	record   pbRecorder.RecorderReadServiceClient
	amqp     *Client
	exchange string

	handlers sync.Map
}

func (s *CasinoService) HandleGameProvideStateChange(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String(), handler)
}

func (s *CasinoService) HandleDealerLogin(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGIN.String(), handler)
}

func (s *CasinoService) HandleDealerLogout(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT.String(), handler)
}

func (s *CasinoService) HandleGameChangingShoe(handler GameProvideHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_GAME_CHANGING_SHOE.String(), handler)
}

func (s *CasinoService) HandleShiftStart(handler GameShiftHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHIFT_START.String(), handler)
}

func (s *CasinoService) HandleShiftEnd(handler GameShiftHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHIFT_END.String(), handler)
}

func (s *CasinoService) HandleShoeStart(handler GameShoeHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHOE_START.String(), handler)
}

func (s *CasinoService) HandleShoeEnd(handler GameShoeHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_SHOE_END.String(), handler)
}

func (s *CasinoService) HandleRoundStart(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_START.String(), handler)
}

func (s *CasinoService) HandleRoundBet(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_BET.String(), handler)
}

func (s *CasinoService) HandleRoundNoMoreBet(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_NO_MORE_BET.String(), handler)
}

func (s *CasinoService) HandleRoundStep(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_STEP.String(), handler)
}

func (s *CasinoService) HandleRoundFinish(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_FINISH.String(), handler)
}

func (s *CasinoService) HandleRoundCancel(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_CANCEL.String(), handler)
}

func (s *CasinoService) HandleRoundPlayback(handler GameRoundHandler) {
	s.handlers.Store(pbRecorder.GameNotifyType_NOTIFY_ROUND_PLAYBACK.String(), handler)
}

func (s *CasinoService) queueName(gameCode string) string {
	return fmt.Sprintf("%v:%v:%v", s.service, s.platform, gameCode)
}

func interfaceToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", value)
	}
}

func (s *CasinoService) genGameHandler(gameCode string) func(Delivery) {
	return func(delivery Delivery) {

		getTraceID := delivery.Headers["traceID"]
		// logrus.Infof("getTraceID:[%v]", getTraceID)
		var infoCtx context.Context
		if getTraceID == nil {
			// logrus.Infof("getTraceID is nil")
			infoCtx, _ = s.tracer.Start(context.Background(), uuid.NewString())
			logrus.AddHook(hook.NewTraceIdHook(uuid.NewString()))
		} else {
			// logrus.Infof("getTraceID is not nil")
			getID := interfaceToString(getTraceID)
			infoCtx, _ = s.tracer.Start(context.Background(), getID)
			logrus.AddHook(hook.NewTraceIdHook(getID))
		}
		ctx := infoCtx

		notifyType := pbRecorder.GameNotifyType(pbRecorder.GameNotifyType_value[delivery.Type])
		switch notifyType {
		case pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGIN,
			pbRecorder.GameNotifyType_NOTIFY_GAME_DEALER_LOGOUT,
			pbRecorder.GameNotifyType_NOTIFY_GAME_CHANGING_SHOE:
			gameProvide := &pbRecorder.GameProvide{}
			err := proto.Unmarshal(delivery.Body, gameProvide)
			if err != nil {
				logrus.WithError(err).Error("receive message but proto unmarshal fail")
			}
			if gameCode != "" && gameProvide.GameCode != gameCode {
				logrus.Error("gameCode not equal with header")
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
				logrus.WithError(err).Error("receive message but proto unmarshal fail")
			}
			if gameCode != "" && shift.GameCode != gameCode {
				logrus.Error("gameCode not equal with header")
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
				logrus.WithError(err).Error("receive message but proto unmarshal fail")
			}
			if gameCode != "" && shoe.GameCode != gameCode {
				logrus.Error("gameCode not equal with header")
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
				logrus.WithError(err).Error("receive message but proto unmarshal fail")
			}
			if gameCode != "" && round.GameCode != gameCode {
				logrus.Error("gameCode not equal with header")
			}
			if value, ok := s.handlers.Load(notifyType.String()); ok {
				if handle, ok := value.(GameRoundHandler); ok {
					handle(ctx, round)
				}
			}
		}
	}
}

func (s *CasinoService) genProvideStateChangeHandler() func(Delivery) {
	return func(delivery Delivery) {
		gameProvide := pbRecorder.GameProvide{}
		err := proto.Unmarshal(delivery.Body, &gameProvide)
		if err != nil {
			logrus.WithError(err).Error("receive message but proto unmarshal fail")
		}

		logrus.Infof("receive delivery headers: %v", delivery.Headers)

		queue := s.queueName(gameProvide.GameCode)
		logrus.Infof("receive gameProvide.State:[%v]", gameProvide.State)
		switch gameProvide.State {
		case pbRecorder.GameProvideState_GAME_PROVIDE_AVAILABLE, pbRecorder.GameProvideState_GAME_PROVIDE_CLOSE_AFTER_ROUND, pbRecorder.GameProvideState_GAME_PROVIDE_MAINTEN_AFTER_ROUND:

			s.amqp.QueueDeclare(QueueDeclare{
				Name:       queue,
				AutoDelete: true,
			})
			s.amqp.QueueBindDeclare(QueueBindDeclare{
				Exchange: s.exchange,
				Queue:    queue,
				Headers: amqp091.Table{
					"x-match":  "all",
					s.platform: true,
					"gameCode": gameProvide.GameCode,
				},
			})
			s.amqp.SubscribeQueue(queue, false, s.genGameHandler(gameProvide.GameCode))
		default:
			s.amqp.RemoveQueueDeclare(queue)
			s.amqp.RemoveQueueBindDeclare(s.exchange, queue)
		}
	}
}

func (s *CasinoService) Start() {

	if s.amqp != nil {
		// logrus.Infof("amqp not nil")
		provideStateQueue := fmt.Sprintf("%v:%v:%v", s.service, s.platform, "notifyProvider")
		// provideStateQueue := "ops@queue"

		s.amqp.QueueDeclare(QueueDeclare{
			Name:       provideStateQueue,
			AutoDelete: true,
		})
		s.amqp.QueueBindDeclare(QueueBindDeclare{
			Exchange: s.exchange,
			Queue:    provideStateQueue,
			Headers: amqp091.Table{
				"x-match":    "all",
				"notifyType": pbRecorder.GameNotifyType_NOTIFY_GAME_PROVIDE_STATE_CHANGE.String(),
				s.platform:   true,
			},
		})

		s.amqp.Connect()

		err := s.amqp.SubscribeQueue(provideStateQueue, false, s.genProvideStateChangeHandler())
		if err != nil {
			logrus.WithError(err).Error("subscribe queue")
		}

	}

}

func (s *CasinoService) End() {
	s.amqp.Close()
}

func NewCasinoService(service, exchange, platform string, provider pbRecorder.ProviderServiceClient, record pbRecorder.RecorderReadServiceClient, amqp *Client) *CasinoService {
	tracerProvider := otel.GetTracerProvider()
	return &CasinoService{
		tracer:   tracerProvider.Tracer("casino-service"),
		service:  service,
		platform: platform,
		provider: provider,
		record:   record,
		amqp:     amqp,
		exchange: exchange,
		handlers: sync.Map{},
	}
}
