import logging
import pika
import pickle


class WorkQueueConnection(object):
    """
    Connection to a remote AMQP queue for sending work requests from WorkQueueClient to WorkQueueProcessor.
    """
    def __init__(self, config):
        work_queue_config = config.work_queue_config()
        self.host = work_queue_config.host
        self.username = work_queue_config.username
        self.password = work_queue_config.password
        self.connection = None

    def connect(self):
        logging.info("Connecting to {} with user {}.".format(self.host, self.username))
        credentials = pika.PlainCredentials(self.username, self.password)
        connection_params = pika.ConnectionParameters(host=self.host, credentials=credentials)
        self.connection = pika.BlockingConnection(connection_params)

    def close(self):
        if self.connection:
            logging.info("Closing connection to {}.".format(self.host))
            self.connection.close()
            self.connection = None

    def create_channel(self, queue_name):
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        return channel

    def send_durable_message(self, queue_name, body):
        self.connect()
        channel = self.create_channel(queue_name)
        resp = channel.basic_publish(exchange='',
                                     routing_key=queue_name,
                                     body=body,
                                     properties=pika.BasicProperties(
                                        delivery_mode=2,  # make message persistent
                                     ))
        self.close()
        return resp

    def receive_loop_with_callback(self, queue_name, callback):
        self.connect()
        channel = self.create_channel(queue_name)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)
        channel.start_consuming()


class WorkRequest(object):
    """
    Request for some operation to be done that is sent over queue from WorkQueueClient to WorkQueueProcessor
    """
    def __init__(self, command, payload):
        """
        Save command name and payload. Both must be serializable.
        :param command: str: name of a command to be run that was created via WorkQueueProcessor.add_command
        :param payload: object: data to be used in running the command
        """
        self.command = command
        self.payload = payload


class WorkQueueClient(object):
    """
    Sends messages to the WorkQueueProcessor via a intermediate AMQP queue.
    """
    def __init__(self, config):
        self.connection = WorkQueueConnection(config)
        self.queue_name = config.work_queue_config().queue_name

    def send(self, command, payload):
        request = WorkRequest(command, payload)
        logging.info("Sending {} message to queue {}.".format(request.command, self.queue_name))
        result = self.connection.send_durable_message(self.queue_name, pickle.dumps(request))
        logging.info("Sent {} message.".format(request.command, self.queue_name))
        return result


class WorkQueueProcessor(object):
    def __init__(self, config):
        self.connection = WorkQueueConnection(config)
        self.queue_name = config.work_queue_config().queue_name
        self.command_name_to_func = {}

    def add_command(self, command, func):
        self.command_name_to_func[command] = func

    def shutdown(self, payload=None):
        logging.info("Work queue shutdown.")
        self.connection.close()

    def process_messages_loop(self):
        try:
            logging.info("Starting work queue loop.")
            self.connection.receive_loop_with_callback(self.queue_name, self.process_message)
        except pika.exceptions.ConnectionClosed as ex:
            logging.error("Connection closed {}.".format(ex))
            raise

    def process_message(self, ch, method, properties, body):
        message = pickle.loads(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        func = self.command_name_to_func.get(message.command)
        if func:
            logging.info("Running command {}.".format(message.command))
            func(message.payload)
        else:
            print("Unknown command: {}".format(message.command))