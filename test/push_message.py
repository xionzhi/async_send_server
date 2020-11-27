import json
import time
import random

import amqpstorm
from amqpstorm.exception import AMQPConnectionError
from flask import Flask, jsonify, request
from yaml import safe_load


class RabbitConnection(object):
    def __init__(self):
        """
        RABBITMQ_HOST: 127.0.0.1
        RABBITMQ_USER: guest
        RABBITMQ_PASSWORD: guest
        RABBITMQ_PORT: 5672
        RABBITMQ_VHOST: /
        RABBITMQ_SIZE: 100
        RABBITMQ_PREFETCH_COUNT: 1
        RABBITMQ_QUEUE: async_send_queue
        """
        self.conf = self.load_yaml_conf()
        self.connection = amqpstorm.Connection(hostname=self.conf['RABBITMQ_HOST'],
                                               username=self.conf['RABBITMQ_USER'],
                                               password=self.conf['RABBITMQ_PASSWORD'],
                                               virtual_host=self.conf['RABBITMQ_VHOST'],
                                               port=self.conf['RABBITMQ_PORT'])
        self.channel = self.connection.channel()
        self.channel.queue.declare(queue=self.conf['RABBITMQ_QUEUE'])

    @staticmethod
    def load_yaml_conf():
        with open('conf/dev.yaml', 'r') as f:
            return safe_load(f)

    def channel_publish(self, callback_data):
        try:
            self.channel.basic.publish(body=json.dumps(callback_data),
                                       routing_key=self.conf['RABBITMQ_QUEUE'],
                                       exchange='')
        except AMQPConnectionError as e:
            self.connection = amqpstorm.Connection(hostname=self.conf['RABBITMQ_HOST'],
                                                   username=self.conf['RABBITMQ_USER'],
                                                   password=self.conf['RABBITMQ_PASSWORD'],
                                                   virtual_host=self.conf['RABBITMQ_VHOST'],
                                                   port=self.conf['RABBITMQ_PORT'])
            self.channel = self.connection.channel()
            self.channel.queue.declare(queue=self.conf['RABBITMQ_QUEUE'])
            logger.error(e)
            self.channel.basic.publish(body=json.dumps(callback_data),
                                       routing_key=self.conf['RABBITMQ_QUEUE'],
                                       exchange='')


app = Flask(__name__)
mq_conn = RabbitConnection()


@app.route('/push')
def push_message():
    message = request.args.get('message', '')

    msg = dict(time_now=int(time.time()),
               message=message,
               user_id=random.randint(1, 4))

    mq_conn.channel_publish(msg)

    return jsonify(msg)


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)
