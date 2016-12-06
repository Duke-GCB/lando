#!/usr/bin/env python
"""
Client to send messages to queue that will be processed by lando.py.
Usage: python lando_client.py <config_filename> <command> <parameter>
Example to run a job: python lando_client.py workerconfig.yml start_worker cwljob.yml
"""
from __future__ import print_function
import os
from lando.messaging.clients import LandoClient
from lando.worker.runworkflow import RunWorkflow
from lando.worker import staging
from lando.exceptions import JobStepFailed


CONFIG_FILE_NAME = '/etc/lando_worker_config.yml'
WORKING_DIR_FORMAT = 'data_for_job_{}'


class LandoWorkerActions(object):
    """
    Functions that handle the actual work for different job steps.
    """
    def __init__(self, config):
        """
        Setup actions with configuration
        :param config: WorkerConfig: settings for use with actions (cwl options)
        """
        self.config = config

    @staticmethod
    def stage_files(working_directory, payload):
        """
        Download files in payload from multiple sources into the working directory.
        :param working_directory: str: path to directory we will save files into
        :param payload: router.StageJobPayload: contains credentials and files to download
        """
        staging_context = staging.Context(payload.credentials)
        for input_file in payload.input_files:
            for dds_file in input_file.dds_files:
                destination_path = os.path.join(working_directory, dds_file.destination_path)
                download_file = staging.DownloadDukeDSFile(dds_file.file_id, destination_path,
                                                           dds_file.agent_id, dds_file.user_id)
                download_file.run(staging_context)
            for url_file in input_file.url_files:
                destination_path = os.path.join(working_directory, url_file.destination_path)
                download_file = staging.DownloadURLFile(url_file.url, destination_path)
                download_file.run(staging_context)

    def run_workflow(self, working_directory, payload):
        """
        Execute workflow specified in payload using data files from working_directory.
        :param working_directory: str: path to directory containing files we will run the workflow using
        :param payload: router.RunJobPayload: details about workflow to run
        """
        cwl_base_command = self.config.cwl_base_command
        run_workflow = RunWorkflow(payload.job_id, working_directory, payload.output_directory, cwl_base_command)
        run_workflow.run_workflow(payload.cwl_file_url, payload.workflow_object_name, payload.input_json)

    @staticmethod
    def save_output(working_directory, payload):
        """
        Upload resulting output directory to a directory in a DukeDS project.
        :param working_directory: str: path to working directory that contains the output directory
        :param payload: path to directory containing files we will run the workflow using
        """
        staging_context = staging.Context(payload.credentials)
        source_directory = os.path.join(working_directory, payload.dir_name)
        upload_folder = staging.UploadDukeDSFolder(payload.project_id, source_directory, payload.dir_name,
                                                   agent_id=payload.dds_app_credentials,
                                                   user_id=payload.dds_user_credentials)
        upload_folder.run(staging_context)


class LandoWorker(object):
    """
    Responds to messages from a queue to run different job steps.
    """
    def __init__(self, config, outgoing_queue_name):
        """
        Setup worker using config to connect to output_queue.
        :param config: WorkerConfig: settings
        :param outgoing_queue_name:
        """
        self.config = config
        self.client = LandoClient(config, outgoing_queue_name)
        self.actions = LandoWorkerActions(config)

    def stage_job(self, payload):
        self.run_job_step_with_func(payload, self.actions.stage_files)

    def run_job(self, payload):
        self.run_job_step_with_func(payload, self.actions.run_workflow)

    def store_job_output(self, payload):
        self.run_job_step_with_func(payload, self.actions.save_output)

    def cancel_job(self, job_id):
        print("TODO implement cancel")

    def run_job_step_with_func(self, payload, func):
        working_directory = WORKING_DIR_FORMAT.format(payload.job_id)
        job_step = JobStep(self.client, payload, func)
        job_step.run(working_directory)


class JobStep(object):
    """
    Displays info, runs the specified function and sends job step complete messages for a job step.
    """
    def __init__(self, client, payload, func):
        """
        Setup job step so we can send a message to client, and run func passing payload.
        :param client: LandoClient: so we can send job step complete message
        :param payload: object: data to be used in this job step
        :param func: func(payload): function we should call before sending complete message
        """
        self.client = client
        self.payload = payload
        self.job_id = payload.job_id
        self.job_description = payload.job_description
        self.func = func

    def run(self, working_directory):
        """
        Run the job step in the specified working directory.
        :param working_directory: str: path to directory which will contain the workflow files
        """
        self.show_start_message()
        try:
            self.func(working_directory, self.payload)
            self.show_complete_message()
            self.send_job_step_complete()
        except JobStepFailed as ex:
            # TODO figure out what to do with ex.details
            print("Job failed:{}".format(ex.message))
            print(ex.details)
            self.send_job_step_errored(ex.message)

    def show_start_message(self):
        """
        Shows message about starting this job step.
        """
        print("{} started for job {}".format(self.job_description, self.job_id))

    def show_complete_message(self):
        """
        Shows message about this job step being completed.
        """
        print("{} complete for job {}.".format(self.job_description, self.job_id))

    def send_job_step_complete(self):
        """
        Sends message back to server that this jobs step is complete.
        """
        self.client.job_step_complete(self.payload)

    def send_job_step_errored(self, message):
        """
        Sends message back to server that this jobs step has failed.
        """
        self.client.job_step_error(self.payload, message)





