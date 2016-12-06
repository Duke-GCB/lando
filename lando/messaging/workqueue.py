"""
Code for processing/sending messages from/to a queue(AMQP)
"""
import logging
import pika
import pickle
from pika.connection import LOGGER as pika_logger


# Disable bogus "Normal shutdown logging.
# fix from https://github.com/pika/pika/issues/719
class LoggerFilterNormalCloseIsFine (logging.Filter):
    def filter (self, record):
        return not record.getMessage().endswith('(200): Normal shutdown')
pika_logger.addFilter(LoggerFilterNormalCloseIsFine())


class WorkQueueConnection(object):
    """
    Connection to a remote AMQP queue for sending work requests from WorkQueueClient to WorkQueueProcessor.
    """
    def __init__(self, config):
        """
        Setup connection with host, username, and password from config.
        :param config: config.Config: contains work queue configuration
        """
        work_queue_config = config.work_queue_config
        self.host = work_queue_config.host
        self.username = work_queue_config.username
        self.password = work_queue_config.password
        self.connection = None

    def connect(self):
        """
        Create internal connection to AMQP service.
        """
        logging.info("Connecting to {} with user {}.".format(self.host, self.username))
        credentials = pika.PlainCredentials(self.username, self.password)
        # Setting heartbeat to 0 so it will be disabled and long running jobs will not timeout.
        connection_params = pika.ConnectionParameters(host=self.host, credentials=credentials, heartbeat_interval=0)
        self.connection = pika.BlockingConnection(connection_params)

    def close(self):
        """
        Close internal connection to AMQP if connected.
        """
        if self.connection:
            logging.info("Closing connection to {}.".format(self.host))
            self.connection.close()
            self.connection = None

    def create_channel(self, queue_name):
        """
        Create a chanel named queue_name. Must be connected before calling this method.
        :param queue_name: str: name of the queue to create
        :return: pika.channel.Channel: channel we can send/receive messages to/from
        """
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        return channel

    def delete_queue(self, queue_name):
        """
        Delete a queue with the specified name.
        :param queue_name:
        :return:
        """
        self.connect()
        channel = self.connection.channel()
        channel.queue_delete(queue=queue_name)
        self.close()


    def send_durable_message(self, queue_name, body):
        """
        Connect to queue_name, post a durable message with body, disconnect from queue_name.
        :param queue_name: str: name of the queue we want to put a message on
        :param body: content of the message we want to send
        """
        self.connect()
        channel = self.create_channel(queue_name)
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=body,
                              properties=pika.BasicProperties(
                                 delivery_mode=2,  # make message persistent
                              ))
        self.close()

    def receive_loop_with_callback(self, queue_name, callback):
        """
        Process incoming messages with callback until close is called.
        :param queue_name: str: name of the queue to poll
        :param callback: func(ch, method, properties, body) called with data when data arrives
        :return:
        """
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
    def __init__(self, config, queue_name):
        """
        Creates connection with host, username, and password from config.
        :param config: config.Config: contains work queue configuration
        """
        self.connection = WorkQueueConnection(config)
        self.queue_name = queue_name

    def send(self, command, payload):
        """
        Send a WorkRequest to containing command and payload to the queue specified in config.
        :param command: str: name of the command we want run by WorkQueueProcessor
        :param payload: object: pickable data to be used when running the command
        """
        request = WorkRequest(command, payload)
        logging.info("Sending {} message to queue {}.".format(request.command, self.queue_name))
        self.connection.send_durable_message(self.queue_name, pickle.dumps(request))
        logging.info("Sent {} message.".format(request.command, self.queue_name))

    def delete_queue(self):
        self.connection.delete_queue(self.queue_name)


class WorkQueueProcessor(object):
    """
    Processes incoming WorkRequest messages from the queue.
    Call add_command to specify operations to run for each WorkRequest.command.
    """
    def __init__(self, config, queue_name):
        """
        Creates connection with host, username, and password from config.
        :param config: config.Config: contains work queue configuration
        """
        self.connection = WorkQueueConnection(config)
        self.queue_name = queue_name
        self.command_name_to_func = {}

    def add_command_by_method_name(self, command, obj):
        """
        Lookup method named command in obj and call that method when the command is received.
        Raises ValueError if obj doesn't have a method named command
        :param command: str: name of the comand to wait for
        :param obj: object: must have a member function with the exact name of the command
        """
        func = getattr(obj, command)
        if func and callable(func):
            self.add_command(command, func)
        else:
            raise ValueError("Object missing {} method.".format(command))

    def add_command(self, command, func):
        """
        Setup func to be run with the WorkRequest.payload when WorkRequest.command == command
        :param command: str: name of the comand to wait for
        :param func: func(object): function to run when the payload arrives.
        """
        self.command_name_to_func[command] = func

    def shutdown(self, payload=None):
        """
        Close the connection/shutdown the messaging loop.
        :param payload: None: not used. Here to allow using this method with add_command.
        """
        logging.info("Work queue shutdown.")
        self.connection.close()

    def process_messages_loop(self):
        """
        Busy loop that processes incoming WorkRequest messages via functions specified by add_command.
        :return:
        """
        try:
            logging.info("Starting work queue loop.")
            self.connection.receive_loop_with_callback(self.queue_name, self.process_message)
        except pika.exceptions.ConnectionClosed as ex:
            logging.error("Connection closed {}.".format(ex))
            raise

    def process_message(self, ch, method, properties, body):
        """
        Callback method that is fired for every message that comes in while we are in process_messages_loop.
        :param ch: channel message was sent on
        :param method: pika.Basic.Deliver
        :param properties: pika.BasicProperties
        :param body: str: payload of the message
        """
        message = pickle.loads(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        func = self.command_name_to_func.get(message.command)
        if func:
            logging.info("Running command {}.".format(message.command))
            func(message.payload)
        else:
            logging.error("Unknown command: {}".format(message.command))
