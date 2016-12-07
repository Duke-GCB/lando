"""
Program that listens for messages on a AMQP queue and runs job steps(staging, run cwl, store output files).
Create config file in /etc/lando_worker_config.yml
Run via script with no arguments: lando_worker
"""
from __future__ import print_function
import os
import sys
import traceback
from lando_messaging.clients import LandoClient
from lando_messaging.messaging import MessageRouter
from lando.worker import cwlworkflow
from lando.worker import staging


CONFIG_FILE_NAME = '/etc/lando_worker_config.yml'
WORKING_DIR_FORMAT = 'data_for_job_{}'


class LandoWorkerSettings(object):
    """
    Creates all external classes for LandoWorker (simplifies unit testing).
    """
    def __init__(self, config):
        self.config = config

    @staticmethod
    def make_lando_client(config, outgoing_queue_name):
        return LandoClient(config, outgoing_queue_name)

    @staticmethod
    def make_staging_context(credentials):
        return staging.Context(credentials)

    @staticmethod
    def make_download_duke_ds_file(file_id, destination_path, user_id):
        return staging.DownloadDukeDSFile(file_id, destination_path, user_id)

    @staticmethod
    def make_download_url_file(url, destination_path):
        return staging.DownloadURLFile(url, destination_path)

    @staticmethod
    def make_cwl_workflow(job_id, working_directory, output_directory, cwl_base_command):
        return cwlworkflow.CwlWorkflow(job_id, working_directory, output_directory, cwl_base_command)

    @staticmethod
    def make_upload_duke_ds_folder(project_id, source_directory, dest_directory, user_id):
        return staging.UploadDukeDSFolder(project_id, source_directory, dest_directory, user_id)


class LandoWorkerActions(object):
    """
    Functions that handle the actual work for different job steps.
    """
    def __init__(self, settings):
        """
        Setup actions with configuration

        """
        self.config = settings.config
        self.settings = settings

    def stage_files(self, working_directory, payload):
        """
        Download files in payload from multiple sources into the working directory.
        :param working_directory: str: path to directory we will save files into
        :param payload: router.StageJobPayload: contains credentials and files to download
        """
        staging_context = self.settings.make_staging_context(payload.credentials)
        for input_file in payload.input_files:
            for dds_file in input_file.dds_files:
                destination_path = os.path.join(working_directory, dds_file.destination_path)
                download_file = self.settings.make_download_duke_ds_file(dds_file.file_id, destination_path,
                                                                         dds_file.user_id)
                download_file.run(staging_context)
            for url_file in input_file.url_files:
                destination_path = os.path.join(working_directory, url_file.destination_path)
                download_file = self.settings.make_download_url_file(url_file.url, destination_path)
                download_file.run(staging_context)

    def run_workflow(self, working_directory, payload):
        """
        Execute workflow specified in payload using data files from working_directory.
        :param working_directory: str: path to directory containing files we will run the workflow using
        :param payload: router.RunJobPayload: details about workflow to run
        """
        cwl_base_command = self.config.cwl_base_command
        workflow = self.settings.make_cwl_workflow(payload.job_id, working_directory, payload.output_directory,
                                                       cwl_base_command)
        workflow.run(payload.cwl_file_url, payload.workflow_object_name, payload.input_json)

    def save_output(self, working_directory, payload):
        """
        Upload resulting output directory to a directory in a DukeDS project.
        :param working_directory: str: path to working directory that contains the output directory
        :param payload: path to directory containing files we will run the workflow using
        """
        staging_context = self.settings.make_staging_context(payload.credentials)
        source_directory = os.path.join(working_directory, payload.dir_name)
        upload_folder = self.settings.make_upload_duke_ds_folder(payload.project_id,
                                                                 source_directory, payload.dir_name,
                                                                 user_id=payload.dds_user_credentials)
        upload_folder.run(staging_context)


class LandoWorker(object):
    """
    Responds to messages from a queue to run different job steps.
    """
    def __init__(self, settings, outgoing_queue_name):
        """
        Setup worker using config to connect to output_queue.
        :param config: WorkerConfig: settings
        :param outgoing_queue_name:
        """
        self.config = settings.config
        self.client = settings.make_lando_client(self.config, outgoing_queue_name)
        self.actions = LandoWorkerActions(settings)

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

    def listen_for_messages(self):
        """
        Blocks and waits for messages on the queue specified in config.
        """
        router = self._make_router()
        print("Lando worker listening for messages on queue '{}'.".format(router.queue_name))
        router.run()

    def _make_router(self):
        work_queue_config = self.config.work_queue_config
        return MessageRouter.make_worker_router(self.config, self, work_queue_config.queue_name)


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
        except: # Trap all exceptions
            tb = traceback.format_exc()
            print("Job failed:{}".format(tb))
            self.send_job_step_errored(tb)

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





