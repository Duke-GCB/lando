
import os
from datetime import datetime
import logging
import json
from lando.server.lando import Lando, JobApi, WorkProgressQueue, WORK_PROGRESS_EXCHANGE_NAME, JobStates, JobSteps
from lando.k8s.cluster import ClusterApi, BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, SecretEnvVar
from lando_messaging.clients import StartJobPayload
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


# class JobActions(object):
#     """
#     Used by LandoRouter to handle messages at a job specific context.
#     """
#     def __init__(self, settings):
#         self.settings = settings
#         self.job_id = settings.job_id
#         self.config = settings.config
#         self.job_api = settings.get_job_api()
#         self.work_progress_queue = settings.get_work_progress_queue()
#         self.cluster_api = settings.get_cluster_api()
#         self.job_name = "job-{}".format(self.job_id)
#         self.volume_name = "job-{}-volume".format(self.job_id)
#         self.bespin_api_secret_name = 'bespin-api-admin'
#         self.cluster_api_secret_name = 'cluster-api'
#         self.rabbit_users_secret_name = 'rabbit-users'
#
#     def start_job(self, payload):
#         job = self.job_api.get_job()
#         self._create_job_data_volume(job)
#         self._create_stage_data_job(job)
#
#     def _create_volume(self, job):
#         volume_claim = self.cluster_api.create_persistent_volume_claim(
#             name=self.volume_name,
#             storage_size_in_g=job.volume_size,
#             storage_class_name=None,
#         )
#         logging.info("Created volume claim {}".format(volume_claim))
#
#     def _create_job(self, job):
#         persistent_claim_volume = PersistentClaimVolume(self.volume_name,
#                                                         mount_path="/data",
#                                                         volume_claim_name=self.volume_name)
#         container = Container(
#             name=self.job_name,
#             image_name=self.config.worker_image_name,
#             command="lando_kube_worker",
#             args=[],
#             working_dir="/data",
#             env_dict={
#                 "JOB_ID": self.job_id,
#                 "WORKFLOW_DIR": "/data",
#                 "BESPIN_API_URL": SecretEnvVar(name=self.bespin_api_secret_name, key='url'),
#                 "BESPIN_API_TOKEN": SecretEnvVar(name=self.bespin_api_secret_name, key='token'),
#                 "BESPIN_CLUSTER_HOST": SecretEnvVar(name=self.cluster_api_secret_name, key='host'),
#                 "BESPIN_CLUSTER_TOKEN": SecretEnvVar(name=self.cluster_api_secret_name, key='token'),
#                 "BESPIN_CLUSTER_NAMESPACE": SecretEnvVar(name=self.cluster_api_secret_name, key='namespace'),
#                 "BESPIN_INCLUSTER_CONFIG": SecretEnvVar(name=self.cluster_api_secret_name, key='incluster_config'),
#                 "BESPIN_RABBIT_HOST": os.environ["BESPIN_RABBIT_HOST"],
#                 "BESPIN_QUEUE_LANDO_USERNAME": SecretEnvVar(self.rabbit_users_secret_name, key='LANDO_USERNAME'),
#                 "BESPIN_QUEUE_LANDO_PASSWORD": SecretEnvVar(self.rabbit_users_secret_name, key='LANDO_PASSWORD'),
#                 "BESPIN_QUEUE_WORKER_USERNAME": SecretEnvVar(self.rabbit_users_secret_name, key='WORKER_USERNAME'),
#                 "BESPIN_QUEUE_WORKER_PASSWORD": SecretEnvVar(self.rabbit_users_secret_name, key='WORKER_PASSWORD'),
#                 "BESPIN_RABBIT_QUEUE": os.environ["BESPIN_RABBIT_QUEUE"],
#             },
#             requested_cpu="100m",
#             requested_memory="64Mi",
#             volumes=[
#                 persistent_claim_volume,
#             ],
#         )
#         job_spec = BatchJobSpec(self.job_name, container=container)
#         job = self.cluster_api.create_job(self.job_name, job_spec)
#         logging.info("Created job {}".format(job))
#
#     def restart_job(self, payload):
#         logging.error("Restart job {}".format(payload))
#
#     def cancel_job(self, payload):
#         logging.error("Cancel job {}".format(payload))
#
#     def store_job_output_complete(self, payload):
#         self.cluster_api.delete_job(self.job_name)
#         self.cluster_api.delete_persistent_volume_claim(self.volume_name)
#         self.job_api.set_job_step(JobSteps.NONE)
#         self.job_api.set_job_state(JobStates.FINISHED)
#
#         # TODO store output project
#
#     def _log_error(self, message):
#         job = self.job_api.get_job()
#         self.job_api.save_error_details(job.step, message)
#
#     def _set_job_state(self, state):
#         self.job_api.set_job_state(state)
#         self._send_job_progress_notification()
#
#     def generic_job_error(self, action_name, details):
#         """
#         Sets current job state to error and creates a job error with the details.
#         :param action_name: str: name of the action that failed
#         :param details: str: details about what went wrong typically a stack trace
#         """
#         self._set_job_state(JobStates.ERRORED)
#         message = "Running {} failed with {}".format(action_name, details)
#         self._show_status(message)
#         self._log_error(message=message)
#
#     def _show_status(self, message):
#         format_str = "{}: {} for job: {}."
#         logging.info(format_str.format(datetime.datetime.now(), message, self.job_id))
#
#     def _send_job_progress_notification(self):
#         job = self.job_api.get_job()
#         payload = json.dumps({
#             "job": job.id,
#             "state": job.state,
#             "step": job.step,
#         })
#         self.work_progress_queue.send(payload)


class JobActions(object):
    """
    Used by LandoRouter to handle messages at a job specific context.
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

    def start_job(self, payload):
        """
        Request from user to start running a job. This starts a job to stage user input data into a volume.
        :param payload:StartJobPayload contains job_id we should start
        """
        self._set_job_state(JobStates.RUNNING)
        manager = self.make_job_manager()

        self._show_status("Creating job data persistent volume")
        manager.create_job_data_persistent_volume(storage_class_name=self.config.storage_class_name)

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
        self._set_job_step(JobSteps.RUNNING)
        manager = self.make_job_manager()
        #TODO manager.cleanup_stage_data_job()

        self._show_status("Creating volumes")
        manager.create_tmpout_persistent_volume(storage_class_name=self.config.storage_class_name)
        manager.create_output_data_persistent_volume(storage_class_name=self.config.storage_class_name)

        self._show_status("Creating run workflow job")
        job = manager.create_run_workflow_job()
        self._show_status("Launched run workflow job: {}".format(job.metadata.name))

    def run_job_complete(self, payload):
        """
        Message from worker that a the run job step is complete and successful.
        Sets the job state to STORING_OUTPUT and puts the store output message into the queue for the worker.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        manager = self.make_job_manager()
        #TODO manager.cleanup_run_workflow_job()

        self._set_job_step(JobSteps.STORING_JOB_OUTPUT)
        self._show_status("Creating store output job")
        job = manager.create_save_output_job()
        self._show_status("Launched save output job: {}".format(job.metadata.name))
        #credentials = self.job_api.get_credentials()
        #job_data = self.job_api.get_store_output_job_data()
        # TODO: run_store_output job

    def store_job_output_complete(self, payload):
        """
        Message from worker that a the store output job step is complete and successful.
        Records information about the resulting output project and frees cloud resources.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        #self.record_output_project_info(payload.output_project_info)

        # TODO: cleanup
        # self._set_job_step(JobSteps.TERMINATE_VM)
        # delete job/pvc/etc
        self._set_job_step(JobSteps.NONE)
        self._set_job_state(JobStates.FINISHED)

    def record_output_project_info(self, output_project_info):
        """
        Records the output project id and readme file id that were created by the worker with the results of the job.
        :param output_project_info: staging.ProjectDetails: info about the project created containing job results
        """
        self._set_job_step(JobSteps.RECORD_OUTPUT_PROJECT)
        #project_id = output_project_info.project_id
        #readme_file_id = output_project_info.readme_file_id
        #self._show_status("Saving project id {} and readme id {}.".format(project_id, readme_file_id))
        #self.job_api.save_project_details(project_id, readme_file_id)
        self._show_status("Terminating VM and queue")
        # TODO cleanup

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


class Names(object):
    def __init__(self, config, job):
        self.volume_name = 'nodidea'




class Paths(object):
    SYSTEM_DATA = '/bespin/system-data'
    JOB_DATA = '/bespin/job-data'
    OUTPUT_DATA = '/bespin/output-data'
    TMPOUT = '/besinp/tmpout'
