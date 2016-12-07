#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: lando_client <command> <parameter>
Example to run a job: lando_client start_job <job_id>
Example to cancel a job: lando_client cancel_job <job_id>
"""
from __future__ import print_function, absolute_import
import sys
from lando.server.config import ServerConfig
from lando.server.lando import LANDO_QUEUE_NAME
from lando_messaging.messaging import JobCommands
from lando_messaging.clients import LandoClient

CONFIG_FILENAME = 'landoconfig.yml'


def main():
    config = ServerConfig(CONFIG_FILENAME)
    client = LandoClient(config, queue_name=LANDO_QUEUE_NAME)
    command = sys.argv[1]
    job_id = int(sys.argv[2])
    if command == JobCommands.START_JOB:
        client.start_job(job_id)
    if command == JobCommands.CANCEL_JOB:
        client.cancel_job(job_id)


if __name__ == '__main__':
    main()
