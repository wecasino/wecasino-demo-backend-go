package weamqp

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

const (
	reconnectDelay = 5 * time.Second

	reInitDelay = 2 * time.Second

	resendDelay = 5 * time.Second
)

var (
	errNotConnected = errors.New("[AMQP] not connected to a server")
	errShutdown     = errors.New("[AMQP] client is shutting down")
)

type ExchangeKind = string

const (
	ExchangeDirect  ExchangeKind = amqp091.ExchangeDirect
	ExchangeTopic   ExchangeKind = amqp091.ExchangeTopic
	ExchangeHeaders ExchangeKind = amqp091.ExchangeHeaders
	ExchangeFanout  ExchangeKind = amqp091.ExchangeFanout
)

type Publishing = amqp091.Publishing
type Config = amqp091.Config
type Delivery = amqp091.Delivery
type Connection = amqp091.Connection

type AtomicBool struct {
	flag int32
}

func (b *AtomicBool) Set(value bool) {
	if value {
		atomic.StoreInt32(&b.flag, 1)
	} else {
		atomic.StoreInt32(&b.flag, 0)
	}
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.flag) != 0
}

type ExchangeDeclare struct {
	Name       string
	Kind       ExchangeKind
	AutoDelete AtomicBool
	exist      AtomicBool
}

type QueueDeclare struct {
	Name       string
	AutoDelete AtomicBool
	exist      AtomicBool
}

type QueueBindDeclare struct {
	Exchange string
	Queue    string
	RouteKey string
	Headers  amqp091.Table
}

type ClientOptions struct {
	Url    url.URL
	Config *Config
}

type subscription struct {
	queue    string
	consumer string
	autoAct  AtomicBool
	handler  func(Delivery)
}

type AMQPPublisher interface {
	Publish(*Publishing) error
	PublishWithContext(context.Context, *Publishing) error
	PublishData([]byte) error
	PublishDataWithContext(context.Context, []byte) error
}

type Client struct {
	ConnectFailHandler func(url.URL) (tryAgain bool, newURL *url.URL)

	url               url.URL
	config            *Config
	exchangeDeclares  sync.Map
	queueDeclares     sync.Map
	queueBindDeclares sync.Map

	_conn    *amqp091.Connection
	_channel *amqp091.Channel

	doneCh           chan AtomicBool
	readyCh          chan AtomicBool
	notifyConnClose  chan *amqp091.Error
	notifyChanClose  chan *amqp091.Error
	notifyConfirm    chan amqp091.Confirmation
	mu               sync.Mutex
	isReady          AtomicBool
	shouldRun        AtomicBool
	mapSubscriptions sync.Map

	doRemove AtomicBool
}

func (client *Client) setReady(ready bool) {
	client.mu.Lock()
	client.isReady.Set(ready)
	client.mu.Unlock()
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp091.Connection) {
	client._conn = connection
	client.notifyConnClose = make(chan *amqp091.Error, 1)
	client._conn.NotifyClose(client.notifyConnClose)
}

// connect will create a new amqp091 connection
func (client *Client) connect() (*amqp091.Connection, error) {

	var conn *amqp091.Connection
	var err error
	if client.config != nil {
		conn, err = amqp091.DialConfig(client.url.String(), *client.config)
	} else {
		conn, err = amqp091.Dial(client.url.String())
	}

	if err != nil {
		logrus.Errorf("[AMQP] connect fail err: [%v]", err)
		return nil, err
	}

	client.changeConnection(conn)
	logrus.Infof("[AMQP] Connected!")
	return conn, nil
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp091.Channel) {
	client._channel = channel
	client.notifyChanClose = make(chan *amqp091.Error, 1)
	client.notifyConfirm = make(chan amqp091.Confirmation, 1)
	client._channel.NotifyClose(client.notifyChanClose)
	client._channel.NotifyPublish(client.notifyConfirm)
}

func (client *Client) GetChannel() *amqp091.Channel {
	return client._channel
}

func (client *Client) GetConn() *amqp091.Connection {
	return client._conn
}

func consume(channel *amqp091.Channel, s subscription) error {
	chDelivery, err := channel.Consume(s.queue, s.consumer, s.autoAct.Get(), false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for delivery := range chDelivery {
			s.handler(delivery)
		}
	}()
	return nil
}

// init will initialize channel & declare queue
func (client *Client) init(conn *amqp091.Connection) error {
	logrus.Infof("[AMQP] init")
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}

	err = client.resetDeclares(ch)

	if err != nil {
		return err
	}

	client.changeChannel(ch)
	client.mapSubscriptions.Range(func(key any, value any) bool {
		_subscription, ok := value.(*subscription)
		if !ok {
			return true
		}
		consume(ch, *_subscription)
		return true
	})
	client.isReady.Set(true)
	logrus.Infof("[AMQP] Setup!")

	return nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp091.Connection) bool {
	for {
		client.isReady.Set(false)

		err := client.init(conn)

		if err != nil {
			logrus.Infof("[AMQP] Failed to initialize channel. Retrying... err:", err)

			select {
			case <-client.doneCh:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		if client.readyCh != nil {
			close(client.readyCh)
			client.readyCh = nil
		}

		select {
		case <-client.doneCh:
			return true
		case err := <-client.notifyConnClose:
			logrus.Infof("[AMQP] Connection closed with error: ", err.Error(), ". Reconnecting...")
			return false
		case <-client.notifyChanClose:
			logrus.Infof("[AMQP] Channel closed. Re-running init...")
		}
	}
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect() {
	for {
		client.isReady.Set(false)
		logrus.Infof("[AMQP] handleReconnect Attempting to connect")

		conn, err := client.connect()
		if err != nil {
			logrus.Infof("[AMQP] Failed to connect. Retrying...")
			select {
			case <-client.doneCh:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if doneCh := client.handleReInit(conn); doneCh {
			break
		}
	}
}

// Public

func (client *Client) declareExchange(declare *ExchangeDeclare) error {
	if client._channel == nil {
		return nil
	}
	passive := declare.exist.Get()
	if passive {
		err := client._channel.ExchangeDeclarePassive(declare.Name, declare.Kind, false, declare.AutoDelete.Get(), false, false, nil)
		if err != nil {
			return err
		}
	} else {
		err := client._channel.ExchangeDeclare(declare.Name, declare.Kind, false, declare.AutoDelete.Get(), false, false, nil)
		if err != nil {
			declare.exist.Set(true)
			passive = true
		}
	}
	return nil
}

func (client *Client) declareQueue(declare *QueueDeclare) error {
	if client._channel == nil {
		return nil
	}
	passive := declare.exist.Get()
	if passive {
		if _, err := client._channel.QueueDeclarePassive(declare.Name, false, declare.AutoDelete.Get(), false, false, nil); err != nil {
			return err
		}
	} else {
		_, err := client._channel.QueueDeclare(declare.Name, false, declare.AutoDelete.Get(), false, false, nil)
		if err != nil {
			declare.exist.Set(true)
			passive = true
		}
	}
	return nil
}

func (client *Client) resetDeclares(channel *amqp091.Channel) error {
	client._channel = channel
	var err error
	client.exchangeDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(*ExchangeDeclare)
		if ok {
			err = client.declareExchange(declare)
			if err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	client.queueDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(*QueueDeclare)
		if ok {
			err = client.declareQueue(declare)
			if err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	client.queueBindDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(*QueueBindDeclare)
		if ok {
			err = client._channel.QueueBind(declare.Queue, declare.RouteKey, declare.Exchange, false, declare.Headers)
			if err != nil {
				return false
			}
		}
		return true
	})
	return err
}

func (client *Client) ExchangeDeclare(declare ExchangeDeclare) error {
	_declare := declare
	client.exchangeDeclares.LoadOrStore(_declare.Name, &_declare)
	return client.declareExchange(&_declare)
}

func (client *Client) QueueDeclare(declare QueueDeclare) error {
	_declare := declare
	client.queueDeclares.LoadOrStore(_declare.Name, &_declare)
	return client.declareQueue(&_declare)
}

func (client *Client) RemoveQueueDeclare(queue string) {
	client.Unsubscribe(queue)
	client.queueDeclares.Delete(queue)
	if client._channel != nil {
		client._channel.QueueDelete(queue, false, false, false)
	}
	time.Sleep(2 * time.Second)
	client.doRemove.Set(false)
}

func (client *Client) RemoveAllQueueDeclare() {
	// 使用 Range 方法遍历 sync.Map
	keys := make([]interface{}, 0)
	client.queueDeclares.Range(func(key, value interface{}) bool {
		keys = append(keys, key)
		return true // 返回 true 继续遍历，返回 false 中止遍历
	})

	for _, item := range keys {
		logrus.Infof("RemoveAllQueueDeclare item:[%v]", item)
		client.RemoveQueueDeclare(item.(string))
	}
}

func (client *Client) QueueBindDeclare(declare QueueBindDeclare) error {
	_declare := declare
	client.queueBindDeclares.LoadOrStore(_declare.Exchange+_declare.Queue, &_declare)
	if client._channel == nil {
		return nil
	}
	return client._channel.QueueBind(declare.Queue, declare.RouteKey, declare.Exchange, false, declare.Headers)
}

func (client *Client) RemoveQueueBindDeclare(exchange, queue string) {
	client.doRemove.Set(true)
	value, loaded := client.queueBindDeclares.LoadAndDelete(exchange + queue)
	if loaded && value != nil && client._channel != nil {
		declare, ok := value.(*QueueBindDeclare)
		if ok {
			client._channel.QueueUnbind(declare.Queue, declare.RouteKey, declare.Exchange, declare.Headers)
		}
	}
}

func (client *Client) RemoveAllQueueBindDeclare(exchange string) {
	// 使用 Range 方法遍历 sync.Map
	keys := make([]interface{}, 0)
	client.queueDeclares.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true // 返回 true 继续遍历，返回 false 中止遍历
	})

	for _, item := range keys {
		logrus.Infof("RemoveAllQueueBindDeclare item:[%v]", item)
		client.RemoveQueueBindDeclare(exchange, item.(string))
	}
}

func (client *Client) Connect() {
	client.mu.Lock()
	_needStart := false
	if !client.shouldRun.Get() {
		client.shouldRun.Set(true)
		client.readyCh = make(chan AtomicBool)
		_needStart = true
	}
	client.mu.Unlock()

	if _needStart {
		go client.handleReconnect()
	}
	if client.readyCh != nil {
		<-client.readyCh
	}
}

// Close will cleanly shutdown the channel and connection.
func (client *Client) Close() error {
	client.mu.Lock()
	_needClose := false
	if client.shouldRun.Get() {
		client.shouldRun.Set(false)
		_needClose = true
	}
	client.mu.Unlock()
	if _needClose {
		client.setReady(false)
		err := client._channel.Close()
		if err != nil {
			return err
		}
		err = client._conn.Close()
		if err != nil {
			return err
		}
		close(client.doneCh)

	}

	return nil
}

func (client *Client) GetReady() bool {
	client.mu.Lock()
	isReady := client.isReady.Get()
	client.mu.Unlock()
	return isReady
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (client *Client) unsafePush(ctx context.Context, exchange, key string, msg *Publishing) error {
	if !client.isReady.Get() {
		return errNotConnected
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return client._channel.PublishWithContext(
		ctx,
		exchange,
		key,
		false,
		false,
		*msg,
	)
}

func (client *Client) Publish(ctx context.Context, exchange, key string, msg *Publishing) error {

	msg.Timestamp = time.Now()
	if !client.isReady.Get() {
		return errors.New("[AMQP] failed to push: not connected, client is not ready")
	}
	for {
		err := client.unsafePush(ctx, exchange, key, msg)
		if err != nil {
			logrus.Infof("[AMQP] Push failed. Retrying...")
			select {
			case <-client.doneCh:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-client.notifyConfirm:
			if confirm.Ack {
				logrus.Infof("[AMQP] Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		logrus.Infof("[AMQP] Push didn't confirm. Retrying...")
	}
}

func (client *Client) Publisher(exchange string) *Publisher {
	return &Publisher{
		exg:    exchange,
		client: client,
	}
}

func (client *Client) SubscribeQueue(ctx context.Context, queue string, autoAct bool, fn func(Delivery)) error {
	// log.Infof("[AMQP] SubscribeQueue")

	client.mu.Lock()
	var setAutoAct = AtomicBool{flag: 0}
	setAutoAct.Set(autoAct)
	subscription := &subscription{
		queue:    queue,
		handler:  fn,
		consumer: queue,
		autoAct:  setAutoAct,
	}
	client.mapSubscriptions.Store(queue, subscription)
	getChannel := client.GetChannel()
	client.mu.Unlock()

	if getChannel != nil {
		// log.Infof("[AMQP] client channel not nil")
		chDelivery, err := getChannel.Consume(queue, "", autoAct, false, false, false, nil)
		if err != nil {
			logrus.Infof("[AMQP] Consume err:[%v]", err)
			return err
		}

		closeChan := make(chan *amqp091.Error)
		client._conn.NotifyClose(closeChan)

		// 循环处理消息
		go func() {
			for {
				select {
				case <-ctx.Done():
					client._conn.Close()
					return //exit loop
				case err = <-closeChan:
					client._conn.Close()
					return //exit loop
				case delivery, ok := <-chDelivery:
					if !ok {
						if client.doRemove.Get() == true {
							logrus.Infof("[AMQP] doRemove")
							return
						}
						logrus.Infof("[AMQP] Consumer channel closed, attempting to reconnect...")
						// 等待一段时间后重新连接
						time.Sleep(1 * time.Second)
						// 重新建立连接
						_, err := client.connect()
						if err != nil {
							logrus.Errorf("[AMQP] Failed to reconnect to RabbitMQ:[%v]", err)
							continue
						}
						chDelivery, err = client._channel.Consume(queue, "", autoAct, false, false, false, nil)
						if err != nil {
							logrus.Errorf("[AMQP] Consume err:[%v]", err)
							return
						}

						if err != nil {
							logrus.Errorf("[AMQP] Failed to register a consumer:[%v]\n", err)
							continue
						}
						// logrus.Infof("[AMQP] Reconnected to RabbitMQ successfully")
						continue
					}

					fn(delivery)
					err = delivery.Ack(false)
					if err != nil {
						logrus.Fatalf("[AMQP] failed ack:[%v]", err)
					}
				}
			}
		}()
	} else {
		logrus.Infof("[AMQP] client channel is nil")
	}

	return nil
}

func (client *Client) Unsubscribe(queue string) {
	_subscription, ok := client.mapSubscriptions.LoadAndDelete(queue)
	if ok && client._channel != nil {
		client._channel.Cancel(_subscription.(*subscription).consumer, false)
	}
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewClient(amqpUrl url.URL, config *Config) *Client {

	_config := Config{}
	if config != nil {
		_config = *config
	}

	client := Client{
		url:              amqpUrl,
		config:           &_config,
		doneCh:           make(chan AtomicBool),
		shouldRun:        AtomicBool{flag: 0},
		isReady:          AtomicBool{flag: 0},
		mapSubscriptions: sync.Map{},
	}
	return &client
}
