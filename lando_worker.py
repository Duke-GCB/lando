#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: python lando_client.py <config_filename> <command> <parameter>
Example to run a job: python lando_client.py workerconfig.yml start_worker cwljob.yml
"""
from __future__ import print_function
import sys
import time
from message_router import MessageRouter, LandoClient, JobCommands
from config import Config
import lando

CONFIG_FILE_NAME = 'workerconfig.yml'

class LandoWorker(object):
    def __init__(self, config, vm_instance_name, queue_name):
        self.config = config
        self.client = LandoClient(config, queue_name)
        self.vm_instance_name = vm_instance_name

    def stage_job(self, payload):
        job_id = payload.job_id
        print("Pretend I am downloading {}".format(job_id))
        for field in payload.fields:
            print("Field", field)
        time.sleep(10)
        self.client.job_step_complete(JobCommands.STAGE_JOB_COMPLETE, job_id, self.vm_instance_name)

    def run_job(self, payload):
        job_id = payload.job_id
        print("Pretend I am running {}".format(job_id))
        for field in payload.fields:
            print("Field", field)

        time.sleep(10)
        self.client.job_step_complete(JobCommands.RUN_JOB_COMPLETE, job_id, self.vm_instance_name)

    def store_job_output(self, payload):
        job_id = payload.job_id
        print("Pretend I am storing output {}".format(job_id))
        for field in payload.fields:
            print("Field", field)
        time.sleep(10)
        self.client.job_step_complete(JobCommands.STORE_JOB_OUTPUT_COMPLETE, job_id, self.vm_instance_name)

    def cancel_job(self, job_id):
        pass


if __name__ == "__main__":
    vm_instance_name = sys.argv[1]
    config = Config(CONFIG_FILE_NAME)
    worker = LandoWorker(config, vm_instance_name, lando.LANDO_QUEUE_NAME)
    MessageRouter.run_worker_router(config, worker, vm_instance_name)


