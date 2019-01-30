import logging
from lando.server.lando import Lando, JobStates, JobSteps, JobSettings, BaseJobActions
from lando.k8s.cluster import ClusterApi
from lando.k8s.jobmanager import JobManager


class K8sJobSettings(JobSettings):
    def get_cluster_api(self):
        settings = self.config.cluster_api_settings
        return ClusterApi(settings.host, settings.token, settings.namespace, verify_ssl=settings.verify_ssl)


class K8sJobActions(BaseJobActions):
    """
    Used by K8sLando to handle messages at a job specific context.
    """
    def __init__(self, settings):
        super(K8sJobActions, self).__init__(settings)
        self.cluster_api = settings.get_cluster_api()

    def make_job_manager(self):
        job = self.job_api.get_job()
        return JobManager(self.cluster_api, self.settings.config, job)

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
        input_files = self.job_api.get_input_files()
        job = manager.create_stage_data_job(input_files)
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

        self._show_status("Creating volumes for running workflow")
        manager.create_run_workflow_persistent_volumes()

        self._show_status("Creating run workflow job")
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
        self._show_status("Marking job finished")
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
        Message from watcher that the staging job had an error
        :param payload:JobStepErrorPayload: info about error
        """
        self._job_step_failed("Staging job failed", payload)

    def run_job_error(self, payload):
        """
        Message from watcher that the run workflow job had an error
        :param payload:JobStepErrorPayload: info about error
        """
        self._job_step_failed("Running job failed", payload)

    def organize_output_error(self, payload):
        """
        Message from watcher that the organize output project job had an error
        :param payload:JobStepErrorPayload: info about error
        """
        self._job_step_failed("Organize output job failed", payload)

    def store_job_output_error(self, payload):
        """
        Message from watcher that the store output project job had an error
        :param payload:JobStepErrorPayload: info about error
        """
        self._job_step_failed("Storing job output failed", payload)

    def _job_step_failed(self, message, payload):
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Storing job output failed")
        self._log_error(message=payload.message)


def create_job_actions(lando, job_id):
    return K8sJobActions(K8sJobSettings(job_id, lando.config))


class K8sLando(Lando):
    def __init__(self, config):
        super(K8sLando, self).__init__(config, create_job_actions)
