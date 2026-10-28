package presence

import (
	"github.com/rabbitmq/amqp091-go"
	"seven.com/sockets/internal/db/rmq"
	"seven.com/sockets/pkg/env"
)

func DeclarePresenceQueues() (*amqp091.Queue, error) {
	dlxName := env.RmqPresenceExName + "_dlx"
	dlqName := env.RmqPresenceQueueName + "_dlq"

	//Dead Letter Queue
	_, dlerr := rmq.DeclareQueue(dlqName, dlxName, env.RmqPresenceDlxRoutingKey, nil)
	if dlerr != nil {
		return nil, dlerr
	}

	qargs := amqp091.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": env.RmqPresenceDlxRoutingKey,
	}

	//Main Queue
	q, err := rmq.DeclareQueue(env.RmqPresenceQueueName, env.RmqPresenceExName, env.RmqPresenceRoutingKey, qargs)
	if err != nil {
		return nil, err
	}

	return q, nil
}
