from workqueue import WorkQueueProcessor, WorkQueueClient


class JobCommands(object):
    START_JOB = 'start_job'                                  # webserver -> lando
    CANCEL_JOB = 'cancel_job'                                # webserver -> lando and lando -> lando_worker

    STAGE_JOB = 'stage_job'                                  # lando -> lando_worker
    STAGE_JOB_COMPLETE = 'stage_job_complete'                # lando_worker -> lando
    STAGE_JOB_ERROR = 'stage_job_error'                      # lando_worker -> lando

    RUN_JOB = 'run_job'                                      # lando -> lando_worker
    RUN_JOB_COMPLETE = 'run_job_complete'                    # lando_worker -> lando
    RUN_JOB_ERROR = 'run_job_error'                          # lando_worker -> lando

    STORE_JOB_OUTPUT = 'store_job_output'                    # lando -> lando_worker
    STORE_JOB_OUTPUT_COMPLETE = 'store_job_output_complete'  # lando_worker -> lando
    STORE_JOB_OUTPUT_ERROR = 'store_job_output_error'        # lando_worker -> lando


LANDO_INCOMING_MESSAGES = [
    JobCommands.START_JOB,
    JobCommands.CANCEL_JOB,
    JobCommands.STAGE_JOB_COMPLETE,
    JobCommands.STAGE_JOB_ERROR,
    JobCommands.RUN_JOB_COMPLETE,
    JobCommands.RUN_JOB_ERROR,
    JobCommands.STORE_JOB_OUTPUT_COMPLETE,
    JobCommands.STORE_JOB_OUTPUT_ERROR
]

LANDO_WORKER_INCOMING_MESSAGES = [
    JobCommands.STAGE_JOB,
    JobCommands.RUN_JOB,
    JobCommands.STORE_JOB_OUTPUT,
]


class MessageRouter(object):
    def __init__(self, config, obj, queue_name, command_names):
        self.processor = WorkQueueProcessor(config, queue_name)
        for command in command_names:
            self.processor.add_command_by_method_name(command, obj)

    def run(self):
        """
        Busy loop that will call commands as messages come in.
        :return:
        """
        self.processor.process_messages_loop()

    @staticmethod
    def run_lando_router(config, obj, queue_name):
        MessageRouter.run_loop(config, obj, queue_name, LANDO_INCOMING_MESSAGES)

    @staticmethod
    def run_worker_router(config, obj, queue_name):
        MessageRouter.run_loop(config, obj, queue_name, LANDO_WORKER_INCOMING_MESSAGES)

    @staticmethod
    def run_loop(config, obj, queue_name, messages):
        router = MessageRouter(config, obj, queue_name, messages)
        router.run()


class LandoClient(object):
    """
    Allows clients of lando to queue messages for lando.
    Two types of clients are supported:
    1) Clients who want lando to run/cancel jobs (Bespin webserver).
    2) Clients who send job progress notifcations (lando_worker.py).
    """
    def __init__(self, config, queue_name):
        self.work_queue_client = WorkQueueClient(config, queue_name)

    def send(self, message, payload):
        self.work_queue_client.send(message, payload)

    def start_job(self, job_id):
        """
        Post a message in the queue that a job in the bespin database be run.
        :param job_id: int: unique id for a job
        """
        self.send(JobCommands.START_JOB, job_id)

    def cancel_job(self, job_id):
        """
        Post a message in the queue that a running job be stopped.
        :param job_id: unique id for a job
        """
        self.send(JobCommands.CANCEL_JOB, job_id)

    def job_step_complete(self, command_name, job_id, vm_instance_name):
        """
        Post a message in the queue that a job step(staging,run, or store output) has finished successfully.
        :param command_name: str: JobCommands.STAGE_JOB_COMPLETE or RUN_JOB_COMPLETE or STORE_JOB_OUTPUT_COMPLETE
        :param job_id: unique id for a job
        :param vm_instance_name: str: name of the vm that is sending this message
        """
        payload = JobStepCompletePayload(job_id, vm_instance_name)
        self.send(command_name, payload)

    def job_step_error(self, command_name, job_id, vm_instance_name, error_message):
        """
        Post a message in the queue that a job step(staging,run, or store output) had an error.
        :param command_name: str: JobCommands.STAGE_JOB_ERROR or RUN_JOB_ERROR or STORE_JOB_OUTPUT_ERROR
        :param job_id: unique id for a job
        :param vm_instance_name: str: name of the vm that is sending this message
        :param error_message: message about the error
        """
        payload = JobStepErrorPayload(job_id, vm_instance_name, error_message)
        self.send(command_name, payload)


class JobStepCompletePayload(object):
    """
    Payload that will be sent to the *_job_complete methods
    """
    def __init__(self, job_id, vm_instance_name):
        self.job_id = job_id
        self.vm_instance_name = vm_instance_name


class JobStepErrorPayload(object):
    """
    Payload that will be sent to the *_job_error methods
    """
    def __init__(self, job_id, vm_instance_name, message):
        self.job_id = job_id
        self.vm_instance_name = vm_instance_name
        self.message = message


class LandoWorkerClient(object):
    """
    Allows clients of lando_worker to queue messages for lando_worker.
    Clients can request various job steps to be run or the job to be canceled.
    """
    def __init__(self, config, queue_name):
        self.work_queue_client = WorkQueueClient(config, queue_name)

    def stage_job(self, credentials, job_id, input_files):
        self.send(JobCommands.STAGE_JOB, StageJobPayload(credentials, job_id, input_files))

    def run_job(self, job_id, workflow):
        self.send(JobCommands.RUN_JOB, RunJobPayload(job_id, workflow))

    def store_job_output(self, credentials, job_id, output_directory):
        self.send(JobCommands.STORE_JOB_OUTPUT, StoreJobOutputPayload(credentials, job_id, output_directory))

    def cancel_job(self, job_id):
        self.send(JobCommands.CANCEL_JOB, job_id)

    def send(self, message, payload):
        self.work_queue_client.send(message, payload)

    def delete_queue(self):
        self.work_queue_client.delete_queue()


class StageJobPayload(object):
    def __init__(self, credentials, job_id, input_files):
        self.credentials = credentials
        self.job_id = job_id
        self.input_files = input_files


class RunJobPayload(object):
    def __init__(self, job_id, workflow):
        self.job_id = job_id
        self.cwl_file_url = workflow.url
        self.workflow_object_name = workflow.object_name
        self.input_json = workflow.input_json
        self.output_directory = workflow.output_directory


class StoreJobOutputPayload(object):
    def __init__(self, credentials, job_id, output_directory):
        self.credentials = credentials
        self.job_id = job_id
        self.dir_name = output_directory.dir_name
        self.project_id = output_directory.project_id
        self.dds_app_credentials = output_directory.dds_app_credentials
        self.dds_user_credentials = output_directory.dds_user_credentials
