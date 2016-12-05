from workqueue import WorkQueueProcessor


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
    def run_worker_router(config, obj, listen_queue_name):
        MessageRouter.run_loop(config, obj, listen_queue_name, LANDO_WORKER_INCOMING_MESSAGES)

    @staticmethod
    def run_loop(config, obj, queue_name, messages):
        router = MessageRouter(config, obj, queue_name, messages)
        router.run()


class StartJobPayload(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.vm_instance_name = None


class CancelJobPayload(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.vm_instance_name = None


class StageJobPayload(object):
    def __init__(self, credentials, job_id, input_files, vm_instance_name):
        self.credentials = credentials
        self.job_id = job_id
        self.input_files = input_files
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.STAGE_JOB_COMPLETE
        self.error_command = JobCommands.STAGE_JOB_ERROR
        self.job_description = "Staging files"


class RunJobPayload(object):
    def __init__(self, job_id, workflow, vm_instance_name):
        self.job_id = job_id
        self.cwl_file_url = workflow.url
        self.workflow_object_name = workflow.object_name
        self.input_json = workflow.input_json
        self.output_directory = workflow.output_directory
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.RUN_JOB_COMPLETE
        self.error_command = JobCommands.RUN_JOB_ERROR
        self.job_description = "Running workflow"


class StoreJobOutputPayload(object):
    def __init__(self, credentials, job_id, output_directory, vm_instance_name):
        self.credentials = credentials
        self.job_id = job_id
        self.dir_name = output_directory.dir_name
        self.project_id = output_directory.project_id
        self.dds_app_credentials = output_directory.dds_app_credentials
        self.dds_user_credentials = output_directory.dds_user_credentials
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.STORE_JOB_OUTPUT_COMPLETE
        self.error_command = JobCommands.STORE_JOB_OUTPUT_ERROR
        self.job_description = "Storing output files"


class JobStepCompletePayload(object):
    """
    Payload that will be sent to the *_job_complete methods
    """
    def __init__(self, payload):
        self.job_id = payload.job_id
        self.vm_instance_name = payload.vm_instance_name


class JobStepErrorPayload(object):
    """
    Payload that will be sent to the *_job_error methods
    """
    def __init__(self, payload, message):
        self.job_id = payload.job_id
        self.vm_instance_name = payload.vm_instance_name
        self.message = message