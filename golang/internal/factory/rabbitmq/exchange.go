package rabbitmq

import (
	"fmt"
	"log"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	queueName    string
	keys         []string
	stop         chan any
}

func NewExchangeMiddleware(exchangeName string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}

	defer func() {
		if err != nil {
			ch.Close()
		}
	}()

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // kind
		true,         // durable
		false,        // auto-delete
		false,        // internal
		false,        // no-wait
		nil,
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}

	for _, key := range keys {
		err = ch.QueueBind(q.Name, key, exchangeName, false, nil)
		if err != nil {
			return nil, m.ErrMessageMiddlewareMessage
		}
	}

	return &exchangeMiddleware{
		conn:         conn,
		channel:      ch,
		exchangeName: exchangeName,
		queueName:    q.Name,
		keys:         keys,
		stop:         make(chan any),
	}, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q *exchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	msgs, err := q.channel.Consume(
		q.queueName, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for {
		select {
		case <-q.stop:
			return nil
		case msg, ok := <-msgs:
			if !ok {
				if q.conn.IsClosed() {
					return m.ErrMessageMiddlewareDisconnected
				}
				return nil
			}
			log.Printf("Received a message: %s", msg.Body)
			callbackFunc(m.Message{Body: string(msg.Body)}, func() { msg.Ack(false) }, func() { msg.Nack(false, true) })
		}
	}
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (q *exchangeMiddleware) StopConsuming() {
	close(q.stop)
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q *exchangeMiddleware) Send(msg m.Message) (err error) {
	for _, key := range q.keys {
		err = q.channel.Publish(
			q.exchangeName, // exchange
			key,            // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})

		if err != nil {
			if q.conn.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (q *exchangeMiddleware) Close() error {
	errChan := q.channel.Close()
	errConn := q.conn.Close()

	if errChan != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
