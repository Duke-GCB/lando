#!/usr/bin/env python
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
        self.cloud_service = CloudService(config)
        self.processor = WorkQueueProcessor(config)
        self.processor.add_command(ServerCommands.START_WORKER, self.start_worker)
        self.processor.add_command(ServerCommands.TERMINATE_WORKER, self.terminate_worker)
        self.processor.add_command(ServerCommands.SHUTDOWN_SERVER, self.processor.shutdown)
        self.worker_config_yml = config.make_worker_config_yml()

    def start_worker(self, payload):
        server_name = str(uuid.uuid4())
        self.show_message("Starting VM {}".format(server_name))
        boot_script = BootScript(yaml_str=payload, worker_config_yml=self.worker_config_yml, server_name=server_name)
        instance, ip_address = self.cloud_service.launch_instance(server_name, boot_script.content)
        self.show_message("Started VM {} with ip:{}".format(server_name, ip_address))

    def terminate_worker(self, payload):
        server_name = payload
        self.cloud_service.terminate_instance(server_name)
        self.show_message("Terminated VM {}.".format(server_name))

    def run(self):
        self.processor.process_messages_loop()

    def show_message(self, message):
        print("{}: {}.".format(datetime.now(), message))

if __name__ == "__main__":
    server = Server(Config(CONFIG_FILE_NAME))
    server.run()
