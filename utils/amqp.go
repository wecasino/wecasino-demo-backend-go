package utils

import (
	"net/url"

	"github.com/wecasino/wecasino-example-backend-go/casino"

	"github.com/sirupsen/logrus"
)

const GAME_EXCHANGE = "game-exchange"

func LoadAMQPClient(loader *EnvLoader, env string) *casino.Client {
	logrus.Infof("LoadAMQPClient env:[%v]", env)
	_url := loader.LoadEnv(env)
	if _url == "" {
		return nil
	}
	logrus.Infof("LoadAMQPClient _url:[%v]", _url)
	amqpUrl, err := url.Parse(_url)
	if err != nil {
		loader.Logger.WithError(err).Errorf("amqp url: %v parse failed", amqpUrl)
		panic(err)
	}
	logrus.Infof("LoadAMQPClient amqpUrl:[%v]", amqpUrl)
	// TODO: config tls if needed

	client := casino.NewClient(amqpUrl, nil, loader.Logger.WithField("package", "util.amqp"))
	return client
}

func ExchangeDeclare(exchange string) casino.ExchangeDeclare {
	return casino.ExchangeDeclare{
		Name:       exchange,
		Kind:       casino.ExchangeHeaders,
		AutoDelete: false,
	}
}
