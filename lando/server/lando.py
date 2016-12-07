#!/usr/bin/env python
"""
Server that starts/terminates VMs based on messages received from a queue.
"""
from __future__ import print_function, absolute_import
import uuid
from datetime import datetime
from lando.server.jobapi import JobApi, JobStates
from lando.server.bootscript import BootScript
from lando.server.cloudservice import CloudService, FakeCloudService
from lando_messaging.clients import LandoWorkerClient
from lando_messaging.messaging import MessageRouter


CONFIG_FILE_NAME = '/etc/lando_config.yml'
LANDO_QUEUE_NAME = 'lando'


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

    def get_cloud_service(self):
        """
        Creates cloud service for creating and deleting VMs.
        If configuration has fake_cloud_service set to True this will create a fake cloud service for debugging purposes.
        :return: CloudService
        """
        if self.config.fake_cloud_service:
            return FakeCloudService(self.config)
        else:
            return CloudService(self.config)

    def get_job_api(self):
        """
        Creates object for communicating with Bespin Job API.
        :return: JobApi
        """
        return JobApi(config=self.config, job_id=self.job_id)

    def get_worker_client(self, queue_name):
        """
        Creates object for sending messages to a worker process.
        :param queue_name: str: name of the queue the worker is listening on
        :return: LandoWorkerClient
        """
        return LandoWorkerClient(self.config, queue_name=queue_name)


class JobActions(object):
    """
    Used by LandoRouter to handle messages at a job specific context.
    """
    def __init__(self, settings):
        self.settings = settings
        self.job_id = settings.job_id
        self.config = settings.config
        self.cloud_service = settings.get_cloud_service()
        self.job_api = settings.get_job_api()

    def make_worker_client(self, vm_instance_name):
        """
        Makes a worker client to talk to the queue associated with a particular worker(vm_instance_name).
        :param vm_instance_name: str: name of the instance and also it's queue name.
        :return: LandoWorkerClient
        """
        return self.settings.get_worker_client(queue_name=vm_instance_name)

    def start_job(self, payload):
        """
        Request from user to start running a job. This is a multi step process.
        First step is to launch the vm and send stage data request.
        Then we wait for stage data complete message.
        :param payload:StartJobPayload contains job_id we should start
        """
        vm_instance_name = self.cloud_service.make_vm_name(self.job_id)
        self.launch_vm(vm_instance_name)
        self.send_stage_job_message(vm_instance_name)

    def launch_vm(self, vm_instance_name):
        """
        Sets job state to creating vm, then creates a new VM with vm_instance_name and gives it a floating IP address.
        :param vm_instance_name: str: name we should assign to the new vm
        """
        self._set_job_state(JobStates.CREATE_VM)
        self._show_status("Creating VM")
        worker_config_yml = self.config.make_worker_config_yml(vm_instance_name)
        boot_script = BootScript(worker_config_yml)
        instance, ip_address = self.cloud_service.launch_instance(vm_instance_name, boot_script.content)
        self._show_status("Launched vm with ip {}".format(ip_address))

    def send_stage_job_message(self, vm_instance_name):
        """
        Sets the job's state to staging and puts the stage job message into the queue for the worker with vm_instance_name.
        :param vm_instance_name: str: name of the instance we will send this message to
        """
        self._set_job_state(JobStates.STAGING)
        self._show_status("Staging data")
        credentials = self.job_api.get_credentials()
        worker_client = self.make_worker_client(vm_instance_name)
        worker_client.stage_job(credentials, self.job_id, self.job_api.get_input_files(), vm_instance_name)

    def stage_job_complete(self, payload):
        """
        Message from worker that a the staging job step is complete and successful.
        Sets the job state to RUNNING and puts the run job message into the queue for the worker.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        self._set_job_state(JobStates.RUNNING)
        self._show_status("Running job")
        job = self.job_api.get_job()
        worker_client = self.make_worker_client(payload.vm_instance_name)
        worker_client.run_job(payload.job_id, job.workflow, payload.vm_instance_name)

    def run_job_complete(self, payload):
        """
        Message from worker that a the run job step is complete and successful.
        Sets the job state to STORING_OUTPUT and puts the store output message into the queue for the worker.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        self._set_job_state(JobStates.STORING_JOB_OUTPUT)
        self._show_status("Storing job output")
        credentials = self.job_api.get_credentials()
        job = self.job_api.get_job()
        worker_client = self.make_worker_client(payload.vm_instance_name)
        worker_client.store_job_output(credentials, payload.job_id, job.output_directory, payload.vm_instance_name)

    def store_job_output_complete(self, payload):
        """
        Message from worker that a the store output job step is complete and successful.
        Sets the job state to finished terminates the worker's instance and deletes the worker's queue.
        :param payload: JobStepCompletePayload: contains job id and vm_instance_name
        """
        self._set_job_state(JobStates.FINISHED)
        self._show_status("Terminating VM and queue")
        self.cloud_service.terminate_instance(payload.vm_instance_name)
        worker_client = self.make_worker_client(payload.vm_instance_name)
        worker_client.delete_queue()

    def cancel_job(self, payload):
        """
        Request from user to cancel a running a job.
        Sets status to canceled and terminates the associated VM and deletes the queue.
        :param payload: CancelJobPayload: contains job id we should cancel
        """
        self._set_job_state(JobStates.CANCELED)
        self._show_status("Canceling job")
        job = self.job_api.get_job()
        self.cloud_service.terminate_instance(job.vm_instance_name)
        worker_client = self.make_worker_client(job.vm_instance_name)
        worker_client.delete_queue()

    def stage_job_error(self, payload):
        """
        Message from worker that it had an error staging data.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Staging job failed")
        self._log_error(message=payload.message)
        # TODO recover?

    def run_job_error(self, payload):
        """
        Message from worker that it had an error running a job.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Running job failed")
        self._log_error(message=payload.message)
        # TODO recover?

    def store_job_output_error(self, payload):
        """
        Message from worker that it had an error storing output.
        :param payload:JobStepErrorPayload: info about error
        """
        self._set_job_state(JobStates.ERRORED)
        self._show_status("Storing job output failed")
        self._log_error(message=payload.message)
        # TODO recover?

    def _log_error(self, message):
        print("TODO log error:{}".format(message))

    def _set_job_state(self, state):
        self.job_api.set_job_state(state)

    def _show_status(self, message):
        format_str = "{}: {} for job: {}."
        print(format_str.format(datetime.now(), message, self.job_id))


class Lando(object):
    """
    Contains base methods for handling messages related to managing/running a workflow.
    Main function is to unpack incoming messages creating a JobActions object for the job id
    and running the appropriate method.
    """
    def __init__(self, config):
        """
        Setup configuration.
        :param config: ServerConfig: settings used by JobActions methods
        """
        self.config = config

    def _make_actions(self, job_id):
        """
        Create JobActions a job specific object for handling messages received on the queue.
        :param job_id: int: unique id for the job
        :return: JobActions: object with methods for processing messages received in listen_for_messages
        """
        return JobActions(self._make_job_settings(job_id, self.config))

    @staticmethod
    def _make_job_settings(job_id, config):
        return JobSettings(job_id, config)

    def __getattr__(self, name):
        """
        Forwards all unhandled methods to a new JobActions object based on payload param
        :param name: str: name of the method we are trying to call
        :return: func(payload): function that will call the appropriate JobActions method
        """
        def action_method(payload):
            action = self._make_actions(payload.job_id)
            getattr(action, name)(payload)
        return action_method

    def listen_for_messages(self):
        """
        Blocks and waits for messages on the queue specified in config.
        """
        router = self._make_router()
        print("Lando listening for messages on queue '{}'.".format(router.queue_name))
        router.run()

    def _make_router(self):
        work_queue_config = self.config.work_queue_config
        return MessageRouter.make_lando_router(self.config, self, work_queue_config.listen_queue)






