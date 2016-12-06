"""
Classes that simplify sending messages to the workqueue.
These send messages that will be recevied by MessageRouter.
"""
from lando.messaging.messaging import JobCommands
from lando.messaging.messaging import StartJobPayload, CancelJobPayload, JobStepCompletePayload, JobStepErrorPayload
from lando.messaging.messaging import StageJobPayload, RunJobPayload, StoreJobOutputPayload
from lando.messaging.workqueue import WorkQueueClient


class LandoClient(object):
    """
    Allows clients of lando to queue messages for lando.
    Two types of clients are supported:
    1) Clients who want lando to run/cancel jobs (Bespin webserver).
    2) Clients who send job progress notifcations (lando_worker.py).
    """
    def __init__(self, config, queue_name):
        """
        Setup connection to the queue based on config.
        :param config: WorkerConfig: info about which queue we will send messages to.
        :param queue_name: str: name of the queue we will send messages to
        """
        self.work_queue_client = WorkQueueClient(config, queue_name)

    def send(self, job_command, payload):
        """
        Post a message in our outgoing queue with of the specified job_command and payload
        :param job_command: str: value from JobCommands
        :param payload: object: appropriate *Payload object for the specified job_command
        """
        self.work_queue_client.send(job_command, payload)

    def start_job(self, job_id):
        """
        Post a message in the queue that a job in the bespin database be run.
        :param job_id: int: unique id for a job
        """
        self.send(JobCommands.START_JOB, StartJobPayload(job_id))

    def cancel_job(self, job_id):
        """
        Post a message in the queue that a running job be stopped.
        :param job_id: unique id for a job
        """
        self.send(JobCommands.CANCEL_JOB, CancelJobPayload(job_id))

    def job_step_complete(self, job_request_payload):
        """
        Send message that the job step is complete using payload data.
        :param job_request_payload: StageJobPayload|RunJobPayload|StoreJobOutputPayload payload from complete job
        """
        payload = JobStepCompletePayload(job_request_payload)
        self.send(job_request_payload.success_command, payload)

    def job_step_error(self, job_request_payload, message):
        """
        Send message that the job step failed using payload data.
        :param job_request_payload: StageJobPayload|RunJobPayload|StoreJobOutputPayload payload from job with error
        :param message: description of the error
        """
        payload = JobStepErrorPayload(job_request_payload, message)
        self.send(job_request_payload.error_command, payload)


class LandoWorkerClient(object):
    """
    Allows queuing messages to control a lando_worker.
    Clients can request various job steps to be run or the job to be canceled.
    """
    def __init__(self, config, queue_name):
        """
        Setup connection to a worker based on config and the specified queue_name.
        :param config: WorkerConfig: info about which queue we will send messages to.
        :param queue_name: str: name of the queue we will send messages to
        """
        self.work_queue_client = WorkQueueClient(config, queue_name)

    def stage_job(self, credentials, job_id, input_files, vm_instance_name):
        """
        Request that a job be staged on a worker(ie. download some files)
        :param credentials: jobapi.Credentials: user's credentials used to download input_files
        :param job_id: int: unique id for this job
        :param input_files: [InputFile]: list of files to download
        :param vm_instance_name: str: name of the instance lando_worker is running on (this passed back in the response)
        """
        self._send(JobCommands.STAGE_JOB, StageJobPayload(credentials, job_id, input_files, vm_instance_name))

    def run_job(self, job_id, workflow, vm_instance_name):
        """
        Execute a workflow on a worker.
        :param job_id: int: unique id for this job
        :param workflow: jobapi.Workflow: url to workflow and parameters to use
        :param vm_instance_name: name of the instance lando_worker is running on (this passed back in the response)
        """
        self._send(JobCommands.RUN_JOB, RunJobPayload(job_id, workflow, vm_instance_name))

    def store_job_output(self, credentials, job_id, output_directory, vm_instance_name):
        """
        Store the output of a finished job.
        :param credentials: jobapi.Credentials: user's credentials used to upload resulting files
        :param job_id: int: unique id for this job
        :param output_directory: jobapi.OutputDirectory: info about where we will upload the results
        :param vm_instance_name: name of the instance lando_worker is running on (this passed back in the response)
        """
        payload = StoreJobOutputPayload(credentials, job_id, output_directory, vm_instance_name)
        self._send(JobCommands.STORE_JOB_OUTPUT, payload)

    def cancel_job(self, job_id):
        """
        Request that the worker cancel a currently running job.
        :param job_id: int: unique id for the job we want to cancel
        """
        self._send(JobCommands.CANCEL_JOB, job_id)

    def _send(self, command, payload):
        """
        Send a message over work queue to worker.
        :param command: str: string from JobCommands for which type of command we are sending
        :param payload: object: payload appropriate for the command we are sending
        """
        self.work_queue_client.send(command, payload)

    def delete_queue(self):
        """
        Delete the queue we are sending messages over.
        This will end the event loop on lando_worker.
        """
        self.work_queue_client.delete_queue()