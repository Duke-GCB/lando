"""
Program that listens for messages on a AMQP queue and runs job steps(staging, run cwl, store output files).
Create config file in /etc/lando_worker_config.yml
Run via script with no arguments: lando_worker
"""

import os
import traceback
import logging
import dateutil
from lando_messaging.clients import LandoClient
from lando_messaging.messaging import MessageRouter
from lando.worker import cwlworkflow
from lando.commands import StageDataCommand, OrganizeOutputCommand, SaveOutputCommand, create_workflow_names


CONFIG_FILE_NAME = '/etc/lando_worker_config.yml'
WORKING_DIR_FORMAT = 'data_for_job_{}'


class LandoWorkerActions(object):
    """
    Functions that handle the actual work for different job steps.
    """
    def __init__(self, config, client):
        """
        Setup actions with configuration
        """
        self.config = config
        self.commands = self.config.commands
        self.client = client

    def stage_files(self, paths, names, payload):
        """
        Download files in payload from multiple sources into the working directory.
        :param working_directory: str: path to directory we will save files into
        :param payload: router.StageJobPayload: contains credentials and files to download
        """
        os.makedirs(paths.CONFIG_DIR, exist_ok=True)
        single_user_id = self.get_single_dds_user_id(payload.input_files)
        dds_credentials = payload.credentials.dds_user_credentials[single_user_id]
        command = StageDataCommand(payload.job_details.workflow, names, paths)
        command.run(self.commands.stage_data_command, dds_credentials, payload.input_files)
        self.client.job_step_complete(payload)

    @staticmethod
    def get_single_dds_user_id(input_files):
        user_id = None
        for dds_file in input_files.dds_files:
            if not user_id:
                user_id = dds_file.user_id
            else:
                if user_id != dds_file.user_id:
                    raise ValueError("ERROR: Found multiple user ids {},{}.".format(user_id, dds_file.user_id))
        if user_id:
            return user_id
        raise ValueError("ERROR: No user_id found in input files.")

    def run_workflow(self, paths, names, payload):
        """
        Execute workflow specified in payload using data files from working_directory.
        :param working_directory: str: path to directory containing files we will run the workflow using
        :param payload: router.RunJobPayload: details about workflow to run
        """
        os.makedirs(paths.OUTPUT_RESULTS_DIR, exist_ok=True)
        cwl_base_command = self.config.cwl_base_command
        cwl_post_process_command = self.config.cwl_post_process_command
        workflow = cwlworkflow.CwlWorkflow(cwl_base_command, cwl_post_process_command, paths.OUTPUT_RESULTS_DIR)

        workflow.run(names.workflow_to_run, names.job_order_path,
                     names.run_workflow_stdout_path, names.run_workflow_stderr_path)
        self.client.job_step_complete(payload)

    def organize_output(self, paths, names, payload):
        command = OrganizeOutputCommand(payload.job_details, names, paths)
        command.run(self.commands.organize_output_command, payload.job_details.workflow.methods_document)
        self.client.job_step_complete(payload)

    def save_output(self, paths, names, payload):
        """
        Upload resulting output directory to a directory in a DukeDS project.
        :param working_directory: str: path to working directory that contains the output directory
        :param payload: path to directory containing files we will run the workflow using
        """
        user_credential_id = payload.job_details.output_project.dds_user_credentials
        credentials = payload.credentials.dds_user_credentials[user_credential_id]
        command = SaveOutputCommand(names, paths, names.activity_name, names.activity_description)
        command.run(self.commands.save_output_command, credentials, payload.job_details.share_dds_ids)
        project_details = command.get_project_details()
        output_project_info = ProjectDetails(project_id=project_details["project_id"],
                                             readme_file_id=project_details["readme_file_id"])
        self.client.job_step_store_output_complete(payload, output_project_info)


class ProjectDetails(object):
    def __init__(self, project_id, readme_file_id):
        self.project_id = project_id
        self.readme_file_id = readme_file_id


class LandoWorker(object):
    """
    Responds to messages from a queue to run different job steps.
    """
    def __init__(self, config, outgoing_queue_name):
        """
        Setup worker using config to connect to output_queue.
        :param config: WorkerConfig: config
        :param outgoing_queue_name:
        """
        self.config = config
        self.client = LandoClient(self.config, outgoing_queue_name)
        self.actions = LandoWorkerActions(config, self.client)

    def stage_job(self, payload):
        self.run_job_step_with_func(payload, self.actions.stage_files)

    def run_job(self, payload):
        self.run_job_step_with_func(payload, self.actions.run_workflow)

    def store_job_output(self, payload):
        self.run_job_step_with_func(payload, self.actions.save_output)

    def organize_output(self, payload):
        self.run_job_step_with_func(payload, self.actions.organize_output)

    def run_job_step_with_func(self, payload, func):
        working_directory = WORKING_DIR_FORMAT.format(payload.job_id)
        if not os.path.exists(working_directory):
            os.mkdir(working_directory)
        job_step = JobStep(self.client, payload, func)
        job_step.run(working_directory)

    def listen_for_messages(self):
        """
        Blocks and waits for messages on the queue specified in config.
        """
        router = self._make_router()
        self.client.worker_started(router.queue_name)
        logging.info("Lando worker listening for messages on queue '{}'.".format(router.queue_name))
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
            paths = Paths(working_directory)
            names = Names(paths, self.payload.job_details)
            self.func(paths, names, self.payload)
            self.show_complete_message()
        except: # Trap all exceptions
            tb = traceback.format_exc()
            logging.info("Job failed:{}".format(tb))
            self.send_job_step_errored(tb)

    def show_start_message(self):
        """
        Shows message about starting this job step.
        """
        logging.info("{} started for job {}".format(self.job_description, self.job_id))

    def show_complete_message(self):
        """
        Shows message about this job step being completed.
        """
        logging.info("{} complete for job {}.".format(self.job_description, self.job_id))

    def send_job_step_errored(self, message):
        """
        Sends message back to server that this jobs step has failed.
        """
        self.client.job_step_error(self.payload, message)


class Names(object):
    def __init__(self, paths, job):
        self.job_order_path = '{}/job-order.json'.format(paths.JOB_DATA)
        self.run_workflow_stdout_path = '{}/bespin-workflow-output.json'.format(paths.OUTPUT_DATA)
        self.run_workflow_stderr_path = '{}/bespin-workflow-output.log'.format(paths.OUTPUT_DATA)
        job_created = dateutil.parser.parse(job.created).strftime("%Y-%m-%d")
        self.output_project_name = "Bespin {} v{} {} {}".format(
            job.workflow.name, job.workflow.version, job.name, job_created)

        self.workflow_input_files_metadata_path = '{}/workflow-input-files-metadata.json'.format(paths.JOB_DATA)
        self.usage_report_path = None  # usage report only available for the calrissian non-VM workflow runner
        self.activity_name = "{} - Bespin Job {}".format(job.name, job.id)
        self.activity_description = "Bespin Job {} - Workflow {} v{}".format(
            job.id, job.workflow.name, job.workflow.version)

        self.stage_data_command_filename = "{}/stage_data.json".format(paths.CONFIG_DIR)
        self.organize_output_command_filename = "{}/organize_output.json".format(paths.CONFIG_DIR)
        self.save_output_command_filename = "{}/save_output.json".format(paths.CONFIG_DIR)
        self.output_project_details_filename = "{}/output_project_details.json".format(paths.CONFIG_DIR)
        self.dds_config_filename = "{}/ddsclient.conf".format(paths.CONFIG_DIR)

        # workflow specific names
        workflow_names = create_workflow_names(job, paths)
        self.workflow_download_dest = workflow_names.workflow_download_dest
        self.workflow_to_run = workflow_names.workflow_to_run
        self.workflow_to_read = workflow_names.workflow_to_read
        self.unzip_workflow_url_to_path = workflow_names.unzip_workflow_url_to_path


class Paths(object):
    def __init__(self, working_directory):
        self.JOB_DATA = '{}/bespin/job-data'.format(working_directory)
        self.WORKFLOW = '{}/bespin/job-data/workflow'.format(working_directory)
        self.CONFIG_DIR = '{}/bespin/config'.format(working_directory)
        self.STAGE_DATA_CONFIG_FILE = '{}/bespin/config/stagedata.json'.format(working_directory)
        self.OUTPUT_DATA = '{}/bespin/output-data'.format(working_directory)
        self.OUTPUT_RESULTS_DIR = '{}/bespin/output-data/results'.format(working_directory)
        self.TMPOUT_DATA = '{}/bespin/output-data/tmpout'.format(working_directory)
        self.REMOTE_README_FILE_PATH = 'results/docs/README.md'
