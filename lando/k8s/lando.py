import logging
import json
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

        self.perform_staging_step()

    def perform_staging_step(self):
        manager = self.make_job_manager()
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

        self.run_workflow_job()

    def run_workflow_job(self):
        manager = self.make_job_manager()
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

        self.organize_output_project()

    def organize_output_project(self):
        manager = self.make_job_manager()
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
        self.save_output()

    def save_output(self):
        store_output_data = self.job_api.get_store_output_job_data()
        # get_store_output_job_data
        manager = self.make_job_manager()
        self._set_job_step(JobSteps.STORING_JOB_OUTPUT)
        self._show_status("Creating store output job")
        job = manager.create_save_output_job(store_output_data.share_dds_ids)
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

        self.record_output_project_info()

        manager = self.make_job_manager()
        manager.cleanup_save_output_job()
        self._show_status("Marking job finished")
        self._set_job_step(JobSteps.NONE)
        self._set_job_state(JobStates.FINISHED)

    def record_output_project_info(self):
        """
        Records the output project id and readme file id that based on the store output pod logs.
        """
        manager = self.make_job_manager()
        self._set_job_step(JobSteps.RECORD_OUTPUT_PROJECT)
        details = json.loads(manager.read_save_output_pod_logs())
        project_id = details['project_id']
        readme_file_id = details.get('readme_file_id', '123')
        self._show_status("Saving project id {} and readme id {}.".format(project_id, readme_file_id))
        self.job_api.save_project_details(project_id, readme_file_id)

    def restart_job(self, payload):
        """
        Request from user to resume running a job. It will resume based on the value of job.step
        returned from the job api. Canceled jobs will always restart from the beginning(vm was terminated).
        :param payload:RestartJobPayload contains job_id we should restart
        """
        job = self.job_api.get_job()
        manager = self.make_job_manager()

        # when to cleanup vs not?
        full_restart = False
        if job.state != JobStates.CANCELED:
            if job.step == JobSteps.CREATE_VM:
                self.cleanup_jobs_and_config_maps()
                self.start_job(None)
            if job.step == JobSteps.STAGING:
                self._set_job_state(JobStates.RUNNING)
                self._set_job_step(JobSteps.RUNNING)
                self.perform_staging_step()
            elif job.step == JobSteps.RUNNING:
                self._set_job_state(JobStates.RUNNING)
                self._set_job_step(JobSteps.RUNNING)
                self.run_workflow_job()
            elif job.step == JobSteps.ORGANIZE_OUTPUT_PROJECT:
                self._set_job_state(JobStates.RUNNING)
                self.organize_output_project()
            elif job.step == JobSteps.STORING_JOB_OUTPUT:
                self._set_job_state(JobStates.RUNNING)
                self.save_output()
            elif job.step == JobSteps.RECORD_OUTPUT_PROJECT:
                self._set_job_state(JobStates.RUNNING)
            else:
                full_restart = True
        else:
            full_restart = True

        if full_restart:
            manager.cleanup_all()
            self.start_job(None)

    def cancel_job(self, payload):
        """
        Request from user to cancel a running a job.
        Sets status to canceled and terminates the associated jobs, configmaps and pvcs
        :param payload: CancelJobPayload: contains job id we should cancel
        """
        self._set_job_step(JobSteps.NONE)
        self._set_job_state(JobStates.CANCELED)
        self._show_status("Canceling job")
        manager = self.make_job_manager()
        manager.cleanup_all()

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
