package utils

import (
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func WaitTillShutDown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logrus.Infoln("shut down start")
}

func GracefulDown(wg *sync.WaitGroup, server *grpc.Server) {
	logrus.Infoln("will stop server gracefully")
	server.GracefulStop()
	wg.Done()
}

func ListenAndServe(server *grpc.Server, addr string) {
	_listener, err := net.Listen("tcp", addr)
	if err != nil {
		logrus.WithError(err).Panicf("net listen err")
	}
	logrus.Infoln("listen on addr: " + addr)

	err = server.Serve(_listener)
	if err != nil {
		logrus.WithError(err).Panicf("server.Serve")
	}
}
