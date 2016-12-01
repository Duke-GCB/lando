#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: python lando_client.py <config_filename> <command> <parameter>
Example to run a job: python lando_client.py workerconfig.yml start_worker cwljob.yml
"""
from __future__ import print_function
import sys
import os
import time
from message_router import MessageRouter, LandoClient, JobCommands
from config import Config
import lando
import staging
from runworkflow import RunWorkflow


CONFIG_FILE_NAME = 'workerconfig.yml'
WORKING_DIR = 'data'


def stage_files(working_directory, payload):
    staging_context = staging.Context(payload.credentials)
    for field in payload.fields:
        if field.type == 'dds_file':
            dds_file = field.dds_file
            destination_path = os.path.join(working_directory, dds_file.path)
            download_file = staging.DownloadDukeDSFile(dds_file.file_id, destination_path,
                                                       dds_file.agent_id, dds_file.user_id)
            download_file.run(staging_context)


def save_output(working_directory, payload):
    print("Uploading files.")
    staging_context = staging.Context(payload.credentials)
    for field in payload.fields:
        if field.type == 'dds_file':
            dds_file = field.dds_file
            source_path = os.path.join(working_directory, dds_file.path)
            download_file = staging.UploadDukeDSFile(dds_file.project_id, source_path, dds_file.path,
                                                     dds_file.agent_id, dds_file.user_id)
            download_file.run(staging_context)
    print("Uploading complete.")


class LandoWorker(object):
    def __init__(self, config, vm_instance_name, queue_name):
        self.config = config
        self.client = LandoClient(config, queue_name)
        self.vm_instance_name = vm_instance_name

    def stage_job(self, payload):
        job_id = payload.job_id
        print("Downloading files for job {}".format(job_id))
        stage_files(WORKING_DIR, payload)
        self.client.job_step_complete(JobCommands.STAGE_JOB_COMPLETE, job_id, self.vm_instance_name)
        print("Downloading files COMPLETE for job {}".format(job_id))

    def run_job(self, payload):
        job_id = payload.job_id
        run_workflow = RunWorkflow(job_id, WORKING_DIR)
        run_workflow.run_workflow(payload.cwl_file_url, payload.workflow_object_name, payload.fields)
        self.client.job_step_complete(JobCommands.RUN_JOB_COMPLETE, job_id, self.vm_instance_name)

    def store_job_output(self, payload):
        job_id = payload.job_id
        save_output(WORKING_DIR, payload)
        self.client.job_step_complete(JobCommands.STORE_JOB_OUTPUT_COMPLETE, job_id, self.vm_instance_name)

    def cancel_job(self, job_id):
        pass


if __name__ == "__main__":
    vm_instance_name = sys.argv[1]
    config = Config(CONFIG_FILE_NAME)
    worker = LandoWorker(config, vm_instance_name, lando.LANDO_QUEUE_NAME)
    MessageRouter.run_worker_router(config, worker, vm_instance_name)


