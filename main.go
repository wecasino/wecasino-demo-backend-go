package main

import (
	"context"
	"encoding/json"

	"github.com/wecasino/wecasino-example-backend-go/utils"
	"github.com/wecasino/wecasino-example-backend-go/utils/track"

	"github.com/wecasino/wecasino-example-backend-go/casino"

	"github.com/sirupsen/logrus"
	pbRecorder "github.com/wecasino/wecasino-proto/pbgo/recorder"
)

const PROGRAM = "CASINO"

const SERVICE = "SERVICE"
const PLATFORM_CODE = "PLATFORM_CODE"

const AMQP_CONNECTION_STRING = "AMQP_CONNECTION_STRING"
const AMQP_EXCHANGE = "AMQP_EXCHANGE"

const PROVIDER_SERVICE_URL = "PROVIDER_SERVICE_URL"

func main() {

	tp, err := track.InitTracer()
	if err != nil {
		logrus.Errorf("track.InitTracer err:[%v]", err)
	}

	ctx := context.Background()
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			logrus.Error("Error shutting down tracer provider: %v", err)
		}
	}()

	recLog := utils.InitLog(PROGRAM)
	envLoader := utils.NewEnvLoader(recLog)
	// envLoader := utils.StanderLoader()

	// 服務
	conn, err := utils.Dial(envLoader.LoadEnvMustNotEmpty(PROVIDER_SERVICE_URL))
	if err != nil {
		logrus.Panic(err)
	}
	_record := pbRecorder.NewRecorderReadServiceClient(conn)
	_provider := pbRecorder.NewProviderServiceClient(conn)

	_service := envLoader.LoadEnvMustNotEmpty(SERVICE)
	platform := envLoader.LoadEnvMustNotEmpty(PLATFORM_CODE)
	exchange := envLoader.LoadEnvOrDefault(AMQP_EXCHANGE, utils.GAME_EXCHANGE)
	_amqp := utils.LoadAMQPClient(envLoader, AMQP_CONNECTION_STRING)

	_casino := casino.NewCasinoService(_service, exchange, platform, _provider, _record, _amqp)

	_casino.HandleGameProvideStateChange(printGameProvide)
	_casino.HandleDealerLogin(printGameProvide)
	_casino.HandleDealerLogout(printGameProvide)
	_casino.HandleGameChangingShoe(printGameProvide)

	_casino.HandleShiftStart(printShift)
	_casino.HandleShiftEnd(printShift)

	_casino.HandleShoeStart(printShoe)
	_casino.HandleShoeEnd(printShoe)

	_casino.HandleRoundStart(printRound)
	_casino.HandleRoundBet(printRound)
	_casino.HandleRoundNoMoreBet(printRound)
	_casino.HandleRoundStep(printRound)
	_casino.HandleRoundFinish(printRound)
	_casino.HandleRoundCancel(printRound)

	_casino.Start()

	// 監聽關機訊號
	utils.WaitTillShutDown()
	_casino.End()
	logrus.Infoln("shut down complete")
}

func printJSON(ctx context.Context, object any) {
	data, err := json.Marshal(object)
	if err != nil {
		logrus.WithError(err).Errorf("json marshal object: %v", object)
	}
	logrus.Print(string(data))

}

func printGameProvide(ctx context.Context, gameProvide *pbRecorder.GameProvide) {
	logrus.WithContext(ctx).Infof("printGameProvide")
	printJSON(ctx, gameProvide)
}

func printShift(ctx context.Context, shift *pbRecorder.ShiftRecord) {
	logrus.WithContext(ctx).Infof("printGameProvide")
	printJSON(ctx, shift)
}

func printShoe(ctx context.Context, shoe *pbRecorder.ShoeRecord) {
	logrus.WithContext(ctx).Infof("printGameProvide")
	printJSON(ctx, shoe)
}

func printRound(ctx context.Context, round *pbRecorder.RoundRecord) {
	logrus.WithContext(ctx).Infof("printGameProvide")
	printJSON(ctx, round)
}
