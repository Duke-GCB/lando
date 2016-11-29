#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: python lando_client.py <config_filename> <command> <parameter>
Example to run a job: python lando_client.py workerconfig.yml start_worker cwljob.yml
"""
from __future__ import print_function
import sys
from workqueue import WorkQueueClient
from lando import ServerCommands
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
    print("Sent {} command.".format(command))


if __name__ == '__main__':
    main()
