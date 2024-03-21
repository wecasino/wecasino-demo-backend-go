package casino

import (
	"context"
	"errors"
	"net/url"
	"sync"
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

type ExchangeDeclare struct {
	Name       string
	Kind       ExchangeKind
	AutoDelete bool
	exist      bool
}

type QueueDeclare struct {
	Name       string
	AutoDelete bool
	exist      bool
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
	Log    *logrus.Entry
}

type subscription struct {
	queue    string
	consumer string
	autoAct  bool
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
	log               *logrus.Entry

	_conn    *amqp091.Connection
	_channel *amqp091.Channel

	done             chan bool
	chReady          chan bool
	notifyConnClose  chan *amqp091.Error
	notifyChanClose  chan *amqp091.Error
	notifyConfirm    chan amqp091.Confirmation
	mu               sync.Mutex
	isReady          bool
	shouldRun        bool
	mapSubscriptions sync.Map
}

func (client *Client) setReady(ready bool) {
	client.mu.Lock()
	client.isReady = ready
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
		return nil, err
	}

	client.changeConnection(conn)
	client.log.Println("[AMQP] Connected!")
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
	chDelivery, err := channel.Consume(s.queue, s.consumer, s.autoAct, false, false, false, nil)
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
	client.log.Println("[AMQP] init")
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
	client.isReady = true
	client.log.Println("[AMQP] Setup!")

	return nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp091.Connection) bool {
	for {
		client.isReady = false

		err := client.init(conn)

		if err != nil {
			client.log.Println("[AMQP] Failed to initialize channel. Retrying... err:", err)

			select {
			case <-client.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		if client.chReady != nil {
			close(client.chReady)
			client.chReady = nil
		}

		select {
		case <-client.done:
			return true
		case err := <-client.notifyConnClose:
			client.log.Println("[AMQP] Connection closed with error: ", err.Error(), ". Reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.log.Println("[AMQP] Channel closed. Re-running init...")
		}
	}
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect() {
	for {
		client.isReady = false
		client.log.Println("[AMQP] handleReconnect Attempting to connect")

		conn, err := client.connect()
		if err != nil {
			client.log.Println("[AMQP] Failed to connect. Retrying...")
			select {
			case <-client.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := client.handleReInit(conn); done {
			break
		}
	}
}

// Public

func (client *Client) declareExchange(declare *ExchangeDeclare) error {
	if client._channel == nil {
		return nil
	}
	passive := declare.exist
	if !passive {
		err := client._channel.ExchangeDeclare(declare.Name, declare.Kind, false, declare.AutoDelete, false, false, nil)
		if err != nil {
			declare.exist = true
			passive = true
		}
	}
	if passive {
		err := client._channel.ExchangeDeclarePassive(declare.Name, declare.Kind, false, declare.AutoDelete, false, false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) declareQueue(declare *QueueDeclare) error {
	if client._channel == nil {
		// client.log.Infof("[AMQP] _channel:[%v]", client._channel)
		return nil
	}
	passive := declare.exist
	// client.log.Infof("[AMQP] passive:[%v]", passive)
	if !passive {
		// client.log.Infof("[AMQP] QueueDeclare name:[%v]", declare.Name)
		_, err := client._channel.QueueDeclare(declare.Name, false, declare.AutoDelete, false, false, nil)
		if err != nil {
			declare.exist = true
			passive = true
		}
	}
	if passive {
		if _, err := client._channel.QueueDeclarePassive(declare.Name, false, declare.AutoDelete, false, false, nil); err != nil {
			return err
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
	value, loaded := client.queueBindDeclares.LoadAndDelete(exchange + queue)
	if loaded && value != nil && client._channel != nil {
		declare, ok := value.(*QueueBindDeclare)
		if ok {
			client._channel.QueueUnbind(declare.Queue, declare.RouteKey, declare.Exchange, declare.Headers)
		}
	}
}

func (client *Client) Connect() {
	client.mu.Lock()
	_needStart := false
	if !client.shouldRun {
		client.shouldRun = true
		client.chReady = make(chan bool)
		_needStart = true
	}
	client.mu.Unlock()

	if _needStart {
		go client.handleReconnect()
	}
	if client.chReady != nil {
		<-client.chReady
	}
}

// Close will cleanly shutdown the channel and connection.
func (client *Client) Close() error {
	client.mu.Lock()
	_needClose := false
	if client.shouldRun {
		client.shouldRun = false
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
		close(client.done)

	}

	return nil
}

func (client *Client) GetReady() bool {
	client.mu.Lock()
	isReady := client.isReady
	client.mu.Unlock()
	return isReady
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (client *Client) unsafePush(ctx context.Context, exchange, key string, msg *Publishing) error {
	if !client.isReady {
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
	if !client.isReady {
		return errors.New("[AMQP] failed to push: not connected, client is not ready")
	}
	for {
		err := client.unsafePush(ctx, exchange, key, msg)
		if err != nil {
			client.log.Println("[AMQP] Push failed. Retrying...")
			select {
			case <-client.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-client.notifyConfirm:
			if confirm.Ack {
				client.log.Println("[AMQP] Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		client.log.Println("[AMQP] Push didn't confirm. Retrying...")
	}
}

func (client *Client) Publisher(exchange string) *Publisher {
	return &Publisher{
		exg:    exchange,
		client: client,
	}
}

func (client *Client) SubscribeQueue(queue string, autoAct bool, fn func(Delivery)) error {
	// client.log.Infof("[AMQP] SubscribeQueue")
	_subscription := &subscription{
		queue:    queue,
		handler:  fn,
		consumer: queue,
		autoAct:  autoAct,
	}
	client.mapSubscriptions.Store(queue, _subscription)
	if client._channel != nil {
		// client.log.Infof("[AMQP] client channel not nil")
		chDelivery, err := client._channel.Consume(queue, "", autoAct, false, false, false, nil)
		if err != nil {
			client.log.WithError(err).Errorf("[AMQP] Consume err")
			return err
		}
		// client.log.Infof("[AMQP] run go func")
		go func() {
			// client.log.Infof("[AMQP] loop call fn delivery")
			for delivery := range chDelivery {
				// client.log.Infof("[AMQP] call delivery func")
				fn(delivery)
				err = delivery.Ack(false)
				if err != nil {
					client.log.Fatalf("failed ack: %v", err)
				}
			}
		}()
	} else {
		client.log.Infof("[AMQP] client channel is nil")
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
func NewClient(amqpUrl *url.URL, config *Config, log *logrus.Entry) *Client {

	_config := Config{}
	if config != nil {
		_config = *config
	}

	client := Client{
		url:              *amqpUrl,
		config:           &_config,
		log:              log,
		done:             make(chan bool),
		shouldRun:        false,
		isReady:          false,
		mapSubscriptions: sync.Map{},
	}
	return &client
}
