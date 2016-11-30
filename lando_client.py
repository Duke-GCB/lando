#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: python lando_client.py <command> <parameter>
Example to run a job: python lando_client.py start_job <job_id>
Example to cancel a job: python lando_client.py cancel_job <job_id>
"""
from __future__ import print_function
import sys
from config import Config
from message_router import LandoClient, JobCommands

CONFIG_FILENAME = 'landoconfig.yml'


def main():
    config = Config(CONFIG_FILENAME)
    client = LandoClient(config, 'lando')
    command = sys.argv[1]
    job_id = int(sys.argv[2])
    if command == JobCommands.START_JOB:
        client.start_job(job_id)
    if command == JobCommands.CANCEL_JOB:
        client.cancel_job(job_id)


if __name__ == '__main__':
    main()
