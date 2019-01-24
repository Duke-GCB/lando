from datetime import datetime
import logging
import json
from lando.server.lando import Lando, JobApi, WorkProgressQueue, WORK_PROGRESS_EXCHANGE_NAME, JobStates, JobSteps
from lando.k8s.cluster import ClusterApi
from lando.k8s.jobmanager import JobManager


class JobSettings(object):
    """
    Creates objects for external communication to be used in JobActions.
    """
    def __init__(self, job_id, config):
        """
        Specifies which job and configuration settings to use
        :param job_id: int: unique id for the job
        :param config: ServerConfig
        """
        self.job_id = job_id
        self.config = config

    def get_job_api(self):
        """
        Creates object for communicating with Bespin Job API.
        :return: JobApi
        """
        return JobApi(config=self.config, job_id=self.job_id)

    def get_work_progress_queue(self):
        """
        Creates object for sending progress notifications to queue containing job progress info.
        """
        return WorkProgressQueue(self.config, WORK_PROGRESS_EXCHANGE_NAME)

    def get_cluster_api(self):
        settings = self.config.cluster_api_settings
        return ClusterApi(settings.host, settings.token, settings.namespace, incluster_config=False,
                          verify_ssl=False)  # TODO REMOVE THIS


class JobActions(object):
    """
    Used by K8sLando to handle messages at a job specific context.
    """
    def __init__(self, settings):
        self.settings = settings
        self.job_id = settings.job_id
        self.config = settings.config
        self.job_api = settings.get_job_api()
        self.work_progress_queue = settings.get_work_progress_queue()
        self.cluster_api = settings.get_cluster_api()

    def make_worker_client(self, vm_instance_name):
        """
        Makes a worker client to talk to the queue associated with a particular worker(vm_instance_name).
        :param vm_instance_name: str: name of the instance and also it's queue name.
        :return: LandoWorkerClient
        """
        return self.settings.get_worker_client(queue_name=vm_instance_name)

    def make_job_manager(self):
        job = self.job_api.get_job()
        return JobManager(self.cluster_api, self.settings, job)

    def job_is_at_state_and_step(self, state, step):
        job = self.job_api.get_job()
        logging.info("Job: {} state:{} step:{}".format(self.job_id, job.state, job.step))
        return job.state == state and job.step == step

    def start_job(self, payload):
        """
        Request from user to start running a job. This starts a job to stage user input data into a volume.
        :param payload:StartJobPayload contains job_id we should start
        """
        self._set_job_state(JobStates.RUNNING)
        self._set_job_step(JobSteps.CREATE_VM)
        manager = self.make_job_manager()

        self._show_status("Creating stage data persistent volumes")
        manager.create_stage_data_persistent_volumes()

        self._set_job_step(JobSteps.STAGING)
        self._show_status("Creating Stage data job")
        for input_file_group in self.job_api.get_input_files():
            job = manager.create_stage_data_job(input_file_group)
            self._show_status("Launched stage data job: {}".format(job.metadata.name))

    def stage_job_complete(self, payload):
        """
        Message from worker that a the staging job step is complete and successful.
        Sets the job state to RUNNING and puts the run job message into the queue for the worker.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        if not self.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.STAGING):
            # ignore request to perform incompatible step
            logging.info("Ignoring request to run job:{} wrong step/state".format(self.job_id))
            return
        self._set_job_step(JobSteps.RUNNING)
        manager = self.make_job_manager()
        self._show_status("Cleaning up after stage data")
        manager.cleanup_stage_data_job()

        self._show_status("Creating volumes for running workflow. Job: {}".format(self.job_id))
        manager.create_run_workflow_persistent_volumes()

        self._show_status("Creating run workflow job")
        self._show_status("Creating job for running workflow. Job: {}".format(self.job_id))
        job = manager.create_run_workflow_job()
        self._show_status("Launched run workflow job: {}".format(job.metadata.name))

    def run_job_complete(self, payload):
        """
        Message from worker that a the run job step is complete and successful.
        Sets the job state to STORING_OUTPUT and puts the store output message into the queue for the worker.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        if not self.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.RUNNING):
            # ignore request to perform incompatible step
            logging.info("Ignoring request to store output for job:{} wrong step/state".format(self.job_id))
            return
        manager = self.make_job_manager()
        manager.cleanup_run_workflow_job()

        self._set_job_step(JobSteps.ORGANIZE_OUTPUT_PROJECT)
        self._show_status("Creating organize output project job")
        job = manager.create_organize_output_project_job()
        self._show_status("Launched organize output project job: {}".format(job.metadata.name))

    def organize_output_complete(self, payload):
        if not self.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.ORGANIZE_OUTPUT_PROJECT):
            # ignore request to perform incompatible step
            logging.info("Ignoring request to organize output project for job:{} wrong step/state".format(self.job_id))
            return
        manager = self.make_job_manager()
        manager.cleanup_organize_output_project_job()

        self._set_job_step(JobSteps.STORING_JOB_OUTPUT)
        self._show_status("Creating store output job")
        job = manager.create_save_output_job()
        self._show_status("Launched save output job: {}".format(job.metadata.name))

    def store_job_output_complete(self, payload):
        """
        Message from worker that a the store output job step is complete and successful.
        Records information about the resulting output project and frees cloud resources.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        if not self.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.STORING_JOB_OUTPUT):
            # ignore request to perform incompatible step
            logging.info("Ignoring request to cleanup for job:{} wrong step/state".format(self.job_id))
            return
        # TODO self.record_output_project_info(payload.output_project_info)
        manager = self.make_job_manager()
        manager.cleanup_save_output_job()

        self._set_job_step(JobSteps.NONE)
        self._set_job_state(JobStates.FINISHED)

    def record_output_project_info(self, output_project_info):
        """
        Records the output project id and readme file id that were created by the worker with the results of the job.
        :param output_project_info: staging.ProjectDetails: info about the project created containing job results
        """
        self._set_job_step(JobSteps.RECORD_OUTPUT_PROJECT)
        project_id = output_project_info.project_id
        readme_file_id = output_project_info.readme_file_id
        self._show_status("Saving project id {} and readme id {}.".format(project_id, readme_file_id))
        self.job_api.save_project_details(project_id, readme_file_id)
        self._show_status("Terminating VM and queue")

    def restart_job(self, payload):
        """
        Request from user to resume running a job. It will resume based on the value of job.step
        returned from the job api. Canceled jobs will always restart from the beginning(vm was terminated).
        :param payload:RestartJobPayload contains job_id we should restart
        """
        job = self.job_api.get_job()
        self.cannot_restart_step_error(step_name=job.step)
        # TODO: figure out how to handle this

    def send_stage_job_message(self, vm_instance_name):
        """
        Sets the job's state to staging and puts the stage job message into the queue for the worker with vm_instance_name.
        :param vm_instance_name: str: name of the instance we will send this message to
        """
        self._set_job_step(JobSteps.STAGING)
        self._show_status("Staging data")
        credentials = self.job_api.get_credentials()
        job = self.job_api.get_job()
        worker_client = self.make_worker_client(vm_instance_name)
        worker_client.stage_job(credentials, job, self.job_api.get_input_files(), vm_instance_name)

    def cancel_job(self, payload):
        """
        Request from user to cancel a running a job.
        Sets status to canceled and terminates the associated VM and deletes the queue.
        :param payload: CancelJobPayload: contains job id we should cancel
        """
        self._set_job_step(JobSteps.NONE)
        self._set_job_state(JobStates.CANCELED)
        self._show_status("Canceling job")
        # TODO: stop running job / cleanup

    def stage_job_error(self, payload):
        """
        Message from worker that it had an error staging data.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Staging job failed")
        self._log_error(message=payload.message)

    def run_job_error(self, payload):
        """
        Message from worker that it had an error running a job.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Running job failed")
        self._log_error(message=payload.message)

    def store_job_output_error(self, payload):
        """
        Message from worker that it had an error storing output.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Storing job output failed")
        self._log_error(message=payload.message)

    def cannot_restart_step_error(self, step_name):
        """
        Set job to error due to trying to restart a job in a step that cannot be restarted.
        :param step_name: str:
        """
        msg = "Cannot restart {} step.".format(step_name)
        self._set_job_state(JobStates.ERRORED)
        self._show_status(msg)
        self._log_error(message=msg)

    def _log_error(self, message):
        job = self.job_api.get_job()
        self.job_api.save_error_details(job.step, message)

    def _set_job_state(self, state):
        self.job_api.set_job_state(state)
        self._send_job_progress_notification()

    def _set_job_step(self, step):
        self.job_api.set_job_step(step)
        if step:
            self._send_job_progress_notification()

    def _send_job_progress_notification(self):
        job = self.job_api.get_job()
        payload = json.dumps({
            "job": job.id,
            "state": job.state,
            "step": job.step,
        })
        self.work_progress_queue.send(payload)

    def _get_cloud_service(self, job):
        return self.settings.get_cloud_service(job.vm_settings)

    def _show_status(self, message):
        format_str = "{}: {} for job: {}."
        logging.info(format_str.format(datetime.now(), message, self.job_id))

    def generic_job_error(self, action_name, details):
        """
        Sets current job state to error and creates a job error with the details.
        :param action_name: str: name of the action that failed
        :param details: str: details about what went wrong typically a stack trace
        """
        self._set_job_state(JobStates.ERRORED)
        message = "Running {} failed with {}".format(action_name, details)
        self._show_status(message)
        self._log_error(message=message)


def create_job_actions(lando, job_id):
    return JobActions(JobSettings(job_id, lando.config))


class K8sLando(Lando):
    def __init__(self, config):
        super(K8sLando, self).__init__(config, create_job_actions)
