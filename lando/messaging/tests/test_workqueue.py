from __future__ import absolute_import
from unittest import TestCase
from lando.dockerutil import DockerRun
from lando.messaging.workqueue import WorkQueueConnection, WorkQueueProcessor, WorkQueueClient
import time

RABBIT_DOCKER = 'rabbitmq:latest'
RABBIT_HOST = "127.0.0.1"
RABBIT_USER = "joe"
RABBIT_PASSWORD = "secret"


class FakeConfig(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.work_queue_config = self


class TestWorkQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        environment = {
            "RABBITMQ_NODENAME": "my-rabbit",
            "RABBITMQ_DEFAULT_USER": RABBIT_USER,
            "RABBITMQ_DEFAULT_PASS": RABBIT_PASSWORD,
        }
        ports = [5672, 15672]
        cls._docker_image = DockerRun(RABBIT_DOCKER, environment, ports)
        cls._docker_image.run()
        time.sleep(10)
        cls.config = FakeConfig(RABBIT_HOST, RABBIT_USER, RABBIT_PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls._docker_image.destroy()
        pass

    def test_work_queue_connection_single_message(self):
        """
        Test that we can send a message through rabbit and receive it on our end.
        """
        my_queue_name = "testing1"
        my_payload = "HEYME"
        work_queue_connection = WorkQueueConnection(self.config)
        work_queue_connection.connect()
        work_queue_connection.send_durable_message(queue_name=my_queue_name, body=my_payload)
        def processor(ch, method, properties, body):
            self.assertEqual(my_payload, body)
            # Terminate receive loop
            work_queue_connection.delete_queue(my_queue_name)
        work_queue_connection.receive_loop_with_callback(queue_name=my_queue_name, callback=processor)

    def test_work_queue_processor(self):
        """
        Make sure we can send and receive messages using higher level WorkQueueProcessor/WorkQueueClient.
        """
        my_queue_name = "testing2"
        client = WorkQueueClient(self.config, my_queue_name)
        processor = WorkQueueProcessor(self.config, my_queue_name)

        # Add three messages processor will run functions for
        processor.add_command("one", self.save_one_value)
        processor.add_command_by_method_name("save_two_value", self)

        def close_queue(payload):
            client.delete_queue()
        processor.add_command("stop", close_queue)

        # Send messages to through rabbitmq
        self.one_value = None
        self.two_value = None
        client.send("one", "oneValue")
        client.send("save_two_value", {'two': 2})
        client.send("stop", 'hey')

        # Wait until close_queue message is processed
        processor.process_messages_loop()
        self.assertEqual(self.one_value, "oneValue")
        self.assertEqual(self.two_value, {'two': 2})

    def save_one_value(self, payload):
        # Saves value for test_work_queue_processor
        self.one_value = payload

    def save_two_value(self, payload):
        # Saves value for test_work_queue_processor
        self.two_value = payload


