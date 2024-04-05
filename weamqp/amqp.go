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
	Passive    bool
	Durable    bool // amqp restart node 不會消失，message 通常需要一併宣告presist
	AutoDelete bool // last consumer cancel or gone
	Internal   bool
	Arguments  amqp091.Table
}

type QueueDeclare struct {
	Name       string
	Passive    bool
	Durable    bool // amqp restart node 不會消失，message 通常需要一併宣告presist
	AutoDelete bool // last consumer cancel or gone
	Exclusive  bool
	Arguments  amqp091.Table
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
	queue     string
	consumer  string
	autoAct   bool
	exclusive bool
	handler   func(Delivery)
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

	conn    *amqp091.Connection
	channel *amqp091.Channel

	doneCh           chan bool
	readyCh          chan bool
	notifyConnClose  chan *amqp091.Error
	notifyChanClose  chan *amqp091.Error
	notifyConfirm    chan amqp091.Confirmation
	mu               sync.Mutex
	isReady          atomic.Bool
	shouldRun        bool
	mapSubscriptions sync.Map
}

// setConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) setConnection(connection *amqp091.Connection) {
	client.conn = connection
	client.notifyConnClose = make(chan *amqp091.Error, 1)
	client.conn.NotifyClose(client.notifyConnClose)
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
		logrus.WithContext(context.TODO()).Errorf("[AMQP]", "connect fail err", err)
		return nil, err
	}

	client.setConnection(conn)
	logrus.WithContext(context.TODO()).Infof("[AMQP]", "Connected")
	return conn, nil
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp091.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp091.Error, 1)
	client.notifyConfirm = make(chan amqp091.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

func consume(channel *amqp091.Channel, s *subscription) error {
	if s == nil {
		return nil
	}
	chDelivery, err := channel.Consume(s.queue, s.consumer, s.autoAct, s.exclusive, false, false, nil)
	if err != nil {
		// logrus.ErrorCtx(context.TODO(), "[AMQP]", "consume err", err)
		logrus.WithContext(context.TODO()).Errorf("[AMQP]", "consume err", err)
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
	// logrus.InfoCtx(context.TODO(), "[AMQP]", "init")
	logrus.WithContext(context.TODO()).Infof("[AMQP]", "init")
	ch, err := conn.Channel()

	if err != nil {
		// logrus.ErrorCtx(context.TODO(), "[AMQP]", "channel err", err)
		logrus.WithContext(context.TODO()).Errorf("[AMQP]", "channel err", err)
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		// logrus.ErrorCtx(context.TODO(), "[AMQP]", "confirm err", err)
		logrus.WithContext(context.TODO()).Errorf("[AMQP]", "confirm err", err)
		return err
	}

	err = client.resetDeclares(ch)
	if err != nil {
		// logrus.ErrorCtx(context.TODO(), "[AMQP]", "resetDeclares err", err)
		logrus.WithContext(context.TODO()).Errorf("[AMQP]", "resetDeclares err", err)
		return err
	}

	client.changeChannel(ch)
	// logrus.InfoCtx(context.TODO(), "[AMQP]", "Setup Range")
	logrus.WithContext(context.TODO()).Infof("[AMQP]", "Setup Range")
	client.mapSubscriptions.Range(func(key any, value any) bool {
		subscription, ok := value.(*subscription)

		if !ok {
			return true
		}
		consume(ch, subscription)
		return true
	})
	client.isReady.Store(true)
	// logrus.InfoCtx(context.TODO(), "[AMQP]", "Setup")
	logrus.WithContext(context.TODO()).Infof("[AMQP]", "Setup")

	return nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp091.Connection) bool {
	for {
		client.isReady.Store(false)

		err := client.init(conn)

		if err != nil {
			// logrus.ErrorCtx(context.TODO(), "[AMQP]", "Failed to initialize channel. Retrying... err", err)
			logrus.WithContext(context.TODO()).Errorf("[AMQP]", "Failed to initialize channel. Retrying... err", err)

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
			// logrus.ErrorCtx(context.TODO(), "[AMQP]", "Connection closed with error", err)
			logrus.WithContext(context.TODO()).Errorf("[AMQP]", "Connection closed with error", err)
			return false
		case <-client.notifyChanClose:
			// logrus.InfoCtx(context.TODO(), "[AMQP]", "Channel closed. Re-running init...")
			logrus.WithContext(context.TODO()).Infof("[AMQP]", "Channel closed. Re-running init...")
		}
	}
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect() {
	for {
		client.isReady.Store(false)

		// logrus.InfoCtx(context.TODO(), "[AMQP]", "handleReconnect Attempting to connect")
		logrus.WithContext(context.TODO()).Infof("[AMQP]", "handleReconnect Attempting to connect")

		conn, err := client.connect()
		if err != nil {
			// logrus.ErrorCtx(context.TODO(), "[AMQP]", "Failed to connect. Retrying.. err", err)
			logrus.WithContext(context.TODO()).Errorf("[AMQP]", "Failed to connect. Retrying.. err", err)
			select {
			case <-client.doneCh:
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

func (client *Client) declareExchange(declare ExchangeDeclare) (bool, error) {

	client.mu.Lock()
	defer client.mu.Unlock()

	if client.channel == nil {
		return false, nil
	}

	passive := declare.Passive
	modify := false
	if !passive {
		err := client.channel.ExchangeDeclare(declare.Name, declare.Kind, declare.Durable, declare.AutoDelete, declare.Internal, false, declare.Arguments)
		if err != nil { // 改使用passive
			passive = true
		}
		modify = true
	}
	if passive {
		err := client.channel.ExchangeDeclarePassive(declare.Name, declare.Kind, declare.Durable, declare.AutoDelete, declare.Internal, false, declare.Arguments)
		return modify, err
	}
	return modify, nil
}

func (client *Client) declareQueue(declare QueueDeclare) (bool, error) {

	client.mu.Lock()
	defer client.mu.Unlock()

	if client.channel == nil {
		return false, nil
	}

	passive := declare.Passive
	modify := false
	if !passive {
		_, err := client.channel.QueueDeclare(declare.Name, declare.Durable, declare.AutoDelete, declare.Exclusive, false, declare.Arguments)
		if err != nil { // 改使用passive
			passive = true
		}
		modify = true
	}
	if passive {
		_, err := client.channel.QueueDeclarePassive(declare.Name, declare.Durable, declare.AutoDelete, declare.Exclusive, false, declare.Arguments)
		return modify, err
	}
	return modify, nil
}

func (client *Client) resetDeclares(channel *amqp091.Channel) error {
	client.channel = channel
	var err error
	client.exchangeDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(ExchangeDeclare)
		if ok {
			updatePassive, errDeclare := client.declareExchange(declare)
			if errDeclare != nil {
				err = errors.Join(err, errDeclare)
				return true
			}
			if updatePassive {
				declare.Passive = true
				client.queueDeclares.Store(key, declare)
			}
		}
		return true
	})
	client.queueDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(QueueDeclare)
		if ok {
			updatePassive, errDeclare := client.declareQueue(declare)
			if errDeclare != nil {
				err = errors.Join(err, errDeclare)
				return true
			}
			if updatePassive {
				declare.Passive = true
				client.queueDeclares.Store(key, declare)
			}
		}
		return true
	})
	client.queueBindDeclares.Range(func(key any, value any) bool {
		declare, ok := value.(QueueBindDeclare)
		if ok {
			err = client.channel.QueueBind(declare.Queue, declare.RouteKey, declare.Exchange, false, declare.Headers)
			if err != nil {
				// logrus.ErrorCtx(context.TODO(), "[AMQP]", "QueueBind error", err)
				logrus.WithContext(context.TODO()).Errorf("[AMQP]", "QueueBind error", err)
				return false
			}
		}
		return true
	})
	return err
}

// Public

func (client *Client) ExchangeDeclare(declare *ExchangeDeclare) error {
	value := *declare
	client.exchangeDeclares.Store(value.Name, value)
	updatePassive, err := client.declareExchange(value)
	if err != nil {
		return err
	}
	if updatePassive {
		declare.Passive = true
		client.exchangeDeclares.Store(value.Name, value)
	}
	return nil
}

func (client *Client) QueueDeclare(declare *QueueDeclare) error {
	value := *declare
	client.queueDeclares.Store(value.Name, value)
	updatePassive, err := client.declareQueue(value)
	if err != nil {
		return err
	}
	if updatePassive {
		declare.Passive = true
		client.queueDeclares.Store(value.Name, value)
	}
	return nil
}

func (client *Client) QueueBindDeclare(declare *QueueBindDeclare) error {
	value := *declare
	client.queueBindDeclares.LoadOrStore(value.Exchange+value.Queue, value)
	if client.channel == nil {
		return nil
	}
	return client.channel.QueueBind(declare.Queue, declare.RouteKey, declare.Exchange, false, declare.Headers)
}

func (client *Client) Connect() {

	client.mu.Lock()
	_needStart := false
	if !client.shouldRun {
		client.shouldRun = true
		client.readyCh = make(chan bool)
		client.doneCh = make(chan bool)
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
	defer client.mu.Unlock()

	_needClose := false
	if client.shouldRun {
		client.shouldRun = false
		_needClose = true
	}
	if _needClose {
		client.isReady.Store(false)
		if client.channel != nil {
			err := client.channel.Close()
			if err != nil {
				return err
			}
		}
		if client.conn != nil {
			err := client.conn.Close()
			if err != nil {
				return err
			}
		}
		if client.doneCh != nil {
			close(client.doneCh)
			client.doneCh = nil
		}

	}

	return nil
}

func (client *Client) SubscribeQueue(ctx context.Context, queue string, autoAct bool, fn func(Delivery)) error {
	// logrus.InfoCtx(ctx, "[AMQP]", "SubscribeQueue", queue)
	logrus.WithContext(context.TODO()).Infof("[AMQP]", "SubscribeQueue", queue)

	subscription := &subscription{
		queue:    queue,
		handler:  fn,
		consumer: queue,
		autoAct:  autoAct,
	}
	client.mapSubscriptions.Store(queue, subscription)

	client.mu.Lock()
	defer client.mu.Unlock()

	channel := client.channel

	if channel != nil {

		err := consume(channel, subscription)
		if err != nil {
			// logrus.ErrorCtx(ctx, "[AMQP]", "Consume err", err)
			logrus.WithContext(context.TODO()).Errorf("[AMQP]", "Consume err", err)
		}
		return err

	} else {
		// logrus.InfoCtx(ctx, "[AMQP]", "client channel is nil", queue)
		logrus.WithContext(context.TODO()).Infof("[AMQP]", "client channel is nil", queue)
	}

	return nil
}

func (client *Client) Unsubscribe(queue string) {
	client.mu.Lock()
	getOne, ok := client.mapSubscriptions.LoadAndDelete(queue)
	if ok && client.channel != nil {
		client.channel.Cancel(getOne.(*subscription).consumer, false)
	}
	client.mu.Unlock()
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewClient(amqpUrl url.URL, config *Config) *Client {

	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	client := Client{
		url:              amqpUrl,
		config:           &cfg,
		doneCh:           make(chan bool, 1),
		shouldRun:        false,
		isReady:          atomic.Bool{},
		mapSubscriptions: sync.Map{},
	}
	return &client
}

func LoadAMQPClient(_url string) (*Client, error) {
	if _url == "" {
		return nil, errors.New("_url is nil")
	}
	amqpUrl, err := url.Parse(_url)
	if err != nil {
		// logrus.Fatalf("amqp url: %v parse failed with error: %v", amqpUrl, err)
		return nil, err
	}
	client := NewClient(*amqpUrl, nil)
	return client, nil
}
