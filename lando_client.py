#!/usr/bin/env python
import sys
from workqueue import WorkQueueClient
from lando_server import ServerCommands
from config import Config


def main():
    config_filename = sys.argv[1]
    command = sys.argv[2]
    payload = ''
    if command == ServerCommands.START_WORKER:
        with open(sys.argv[3], 'r') as infile:
            payload = infile.read()
    if command == ServerCommands.TERMINATE_WORKER:
        payload = sys.argv[3]
    client = WorkQueueClient(Config(config_filename))
    client.send(command, payload)


if __name__ == '__main__':
    main()
