package utils

import (
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const MAX_MESSAGE_SIZE = 20 * 1024 * 1024

var (
	DefaultServiceConfigJson = `{
		"methodConfig": [{
		  "waitForReady": true,
		  "retryPolicy": {
			  "MaxAttempts": 4,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".01s",
			  "BackoffMultiplier": 1.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`
)

func NewServer(opt ...grpc.ServerOption) *grpc.Server {
	serverOpts := []grpc.ServerOption{}
	keepaliveParams := grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 2 * time.Minute,
	})

	serverOpts = append(serverOpts, keepaliveParams, grpc.MaxRecvMsgSize(MAX_MESSAGE_SIZE), grpc.MaxSendMsgSize(MAX_MESSAGE_SIZE))
	serverOpts = append(serverOpts, opt...)
	return grpc.NewServer(serverOpts...)
}

func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {

	credential := grpc.WithTransportCredentials(insecure.NewCredentials())
	if strings.HasSuffix(target, ":443") {
		creds, err := credentials.NewClientTLSFromFile("cert.crt", "")
		if err != nil {
			return nil, err
		}
		credential = grpc.WithTransportCredentials(creds)
	}
	defaultServiceConfig := grpc.WithDefaultServiceConfig(DefaultServiceConfigJson)
	msgMaxOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE),
		grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE),
	)
	keepalive := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time: 5 * time.Minute,
	})
	_opts := []grpc.DialOption{credential, defaultServiceConfig, msgMaxOptions, keepalive, grpc.WithBlock()}
	_opts = append(_opts, opts...)
	entry := logrus.WithField("target", target)
	entry.Infof("start dial to target... %s", target)
	conn, err := grpc.Dial(target, _opts...)
	if err != nil {
		entry.WithError(err).Error("dial to target fail")
		return nil, err
	}
	entry.Info("dial to target success")
	return conn, err
}
