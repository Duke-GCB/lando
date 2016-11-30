#!/usr/bin/env python
"""
Server that starts/terminates VMs based on messages received from a queue.
"""
from __future__ import print_function
from bootscript import BootScript
from cloudservice import CloudService
from config import Config
import uuid
from datetime import datetime
from jobapi import JobApi, JobStates, StagingTypes
from message_router import MessageRouter, LandoWorkerClient
import time
import lando_worker

CONFIG_FILE_NAME = 'landoconfig.yml'
LANDO_QUEUE_NAME = 'lando'


class Lando(object):
    def __init__(self, config):
        """
        Setup cloud service and work queue based on config.
        :param config: config.Config: settings used to connect to AMQP and cloud provider
        """
        self.config = config
        self.cloud_service = CloudService(config)
        self.worker_config_yml = config.make_worker_config_yml()

    def make_worker_client(self, vm_instance_name):
        return LandoWorkerClient(self.config, queue_name=vm_instance_name)

    def start_job(self, job_id):
        #vm_instance_name = str(uuid.uuid4())
        vm_instance_name = 'worker_1'
        self.show_message("Starting vm {}".format(job_id))

        job_api = JobApi(config=self.config, job_id=job_id)
        job_api.set_job_state(JobStates.CREATE_VM)
        time.sleep(2)
        job_api.set_vm_instance_name(vm_instance_name)

        worker = self.make_worker_client(vm_instance_name)
        worker.stage_job(job_id, job_api.get_job_fields(StagingTypes.INPUT))
        job_api.set_job_state(JobStates.STAGING)
        self.show_message("Sent message to {} to stage data".format(vm_instance_name))

    def cancel_job(self, job_id):
        self.show_message("Cancel job {}".format(job_id))

    def stage_job_complete(self, payload):
        self.show_message("Staging complete {}".format(payload))
        worker = self.make_worker_client(payload.vm_instance_name)
        job_api = JobApi(config=self.config, job_id=payload.job_id)
        worker.run_job(payload.job_id, 'somefile.cwl', job_api.get_job_fields())
        job_api.set_job_state(JobStates.RUNNING)

    def stage_job_error(self, payload):
        self.show_message("Staging error {}".format(payload))

    def run_job_complete(self, payload):
        self.show_message("Run complete {}".format(payload))
        worker = self.make_worker_client(payload.vm_instance_name)
        job_api = JobApi(config=self.config, job_id=payload.job_id)
        worker.store_job_output(payload.job_id, job_api.get_job_fields(StagingTypes.OUTPUT))
        job_api.set_job_state(JobStates.STORING_JOB_OUTPUT)

    def run_job_error(self, payload):
        self.show_message("Run job error {}".format(payload))

    def store_job_output_complete(self, payload):
        self.show_message("Store output complete {}".format(payload))
        self.show_message("KILL VM")
        job_api = JobApi(config=self.config, job_id=payload.job_id)
        job_api.set_job_state(JobStates.FINISHED)
        worker = self.make_worker_client(payload.vm_instance_name)
        worker.delete_queue()

    def store_job_output_error(self, payload):
        self.show_message("store job output error {}".format(payload))

    def show_message(self, message):
        """
        Show a messsage on the console with a timestamp.
        :param message: str: message to show
        """
        print("{}: {}.".format(datetime.now(), message))


if __name__ == "__main__":
    config = Config(CONFIG_FILE_NAME)
    lando = Lando(config)
    lando.show_message("Listening for messages...")
    MessageRouter.run_lando_router(config, lando, 'lando')
