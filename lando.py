#!/usr/bin/env python
"""
Server that starts/terminates VMs based on messages received from a queue.
"""
from __future__ import print_function
from workqueue import WorkQueueProcessor
from bootscript import BootScript
from cloudservice import CloudService
from config import Config
import uuid
from datetime import datetime

CONFIG_FILE_NAME = 'landoconfig.yml'


class ServerCommands:
    START_WORKER = 'start_worker'
    TERMINATE_WORKER = 'terminate_worker'
    SHUTDOWN_SERVER = 'shutdown_server'


class Server(object):
    def __init__(self, config):
        """
        Setup cloud service and work queue based on config.
        :param config: config.Config: settings used to connect to AMPQ and cloud provider
        """
        self.cloud_service = CloudService(config)
        self.processor = WorkQueueProcessor(config)
        self.processor.add_command(ServerCommands.START_WORKER, self.start_worker)
        self.processor.add_command(ServerCommands.TERMINATE_WORKER, self.terminate_worker)
        self.processor.add_command(ServerCommands.SHUTDOWN_SERVER, self.processor.shutdown)
        self.worker_config_yml = config.make_worker_config_yml()

    def start_worker(self, payload):
        """
        Called when we receive the ServerCommands.START_WORKER message.
        Creates a new VM that runs a bash script that will run the cwl workflow specified in payload.
        :param payload: yaml settings for running a workflow
        """
        server_name = str(uuid.uuid4())
        self.show_message("Starting VM {}".format(server_name))
        boot_script = BootScript(yaml_str=payload, worker_config_yml=self.worker_config_yml, server_name=server_name)
        instance, ip_address = self.cloud_service.launch_instance(server_name, boot_script.content)
        self.show_message("Started VM {} with ip:{}".format(server_name, ip_address))

    def terminate_worker(self, server_name):
        """
        Called when we receive the ServerCommands.TERMINATE_WORKER message.
        Terminates the VM for the specified server name.
        :param server_name: str: name of the server to terminate
        """
        self.cloud_service.terminate_instance(server_name)
        self.show_message("Terminated VM {}.".format(server_name))

    def run(self):
        """
        Busy loop that will call commands as messages come in.
        :return:
        """
        self.show_message("Listening for messages...")
        self.processor.process_messages_loop()

    def show_message(self, message):
        """
        Show a messsage on the console with a timestamp.
        :param message: str: message to show
        """
        print("{}: {}.".format(datetime.now(), message))

if __name__ == "__main__":
    server = Server(Config(CONFIG_FILE_NAME))
    server.run()
