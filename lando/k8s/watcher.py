from lando.k8s.cluster import ClusterApi, JobConditionType
from lando.k8s.config import create_server_config
from lando.k8s.jobmanager import JobLabels, JobStepTypes
from lando_messaging.clients import LandoClient
from lando_messaging.messaging import JobCommands
import logging
import sys


COMPLETE_JOB_STEP_TO_COMMAND = {
    JobStepTypes.STAGE_DATA: JobCommands.STAGE_JOB_COMPLETE,
    JobStepTypes.RUN_WORKFLOW: JobCommands.RUN_JOB_COMPLETE,
    JobStepTypes.ORGANIZE_OUTPUT: JobCommands.ORGANIZE_OUTPUT_COMPLETE,
    JobStepTypes.SAVE_OUTPUT: JobCommands.STORE_JOB_OUTPUT_COMPLETE
}

ERROR_JOB_STEP_TO_COMMAND = {
    JobStepTypes.STAGE_DATA: JobCommands.STAGE_JOB_ERROR,
    JobStepTypes.RUN_WORKFLOW: JobCommands.RUN_JOB_ERROR,
    JobStepTypes.ORGANIZE_OUTPUT: JobCommands.ORGANIZE_OUTPUT_ERROR,
    JobStepTypes.SAVE_OUTPUT: JobCommands.STORE_JOB_OUTPUT_ERROR,
}


def check_condition_status(job, condition_type):
    conditions = job.status.conditions
    if conditions:
        for condition in conditions:
            if condition.type == condition_type and condition.status:
                return True
    return False


class JobStepPayload(object):
    def __init__(self, job_id, vm_instance_name, success_command):
        self.job_id = job_id
        self.vm_instance_name = vm_instance_name
        self.success_command = success_command


class JobWatcher(object):
    def __init__(self, config):
        self.config = config
        self.cluster_api = self.get_cluster_api(config)
        self.lando_client = LandoClient(config, config.work_queue_config.listen_queue)

    @staticmethod
    def get_cluster_api(config):
        settings = config.cluster_api_settings
        return ClusterApi(settings.host, settings.token, settings.namespace,
                          verify_ssl=False)  # TODO REMOVE THIS

    def run(self):
        # run on_job_change for jobs that have the bespin job label
        bespin_job_label_selector = "{}={}".format(JobLabels.BESPIN_JOB, "true")
        self.cluster_api.wait_for_job_events(self.on_job_change,
                                             label_selector=bespin_job_label_selector)

    def on_job_change(self, job):
        if check_condition_status(job, JobConditionType.COMPLETE):
            self.on_job_succeeded(job)
        elif check_condition_status(job, JobConditionType.FAILED):
            self.on_job_failed(job)
        # TODO cleanup handling this
        self.lando_client.work_queue_client.connection.close()

    def on_job_succeeded(self, job):
        bespin_job_id = job.metadata.labels.get(JobLabels.JOB_ID)
        bespin_job_step = job.metadata.labels.get(JobLabels.STEP_TYPE)
        if bespin_job_id and bespin_job_step:
            self.send_step_complete_message(bespin_job_step, bespin_job_id)

    def send_step_complete_message(self, bespin_job_step, bespin_job_id):
        print("\n\nsend complete for job: {} step: {}".format(bespin_job_id, bespin_job_step))
        job_command = COMPLETE_JOB_STEP_TO_COMMAND.get(bespin_job_step)
        if job_command:
            payload = JobStepPayload(bespin_job_id, None, job_command)
            if job_command == JobCommands.STORE_JOB_OUTPUT_COMPLETE:
                self.lando_client.job_step_store_output_complete(payload, None)
            else:
                self.lando_client.job_step_complete(payload)
        else:
            logging.error("Unable to find job command:", bespin_job_step, bespin_job_id)

    def on_job_failed(self, job):
        bespin_job_id = job.metadata.labels.get(JobLabels.JOB_ID)
        bespin_job_step = job.metadata.labels.get(JobLabels.STEP_TYPE)
        if bespin_job_id and bespin_job_step:
            logs = self.cluster_api.read_pod_logs(job.metadata.name)
            self.send_step_error_message(bespin_job_step, bespin_job_id, message=logs)

    def send_step_error_message(self, bespin_job_step, bespin_job_id, error_message):
        job_command = ERROR_JOB_STEP_TO_COMMAND.get(bespin_job_step)
        if job_command:
            payload = JobStepPayload(bespin_job_id, None, job_command)
            self.lando_client.job_step_error(payload, error_message)
        else:
            logging.error("Unable to find job command:", bespin_job_step, bespin_job_id)


def main():
    config = create_server_config(sys.argv[1])
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    watcher = JobWatcher(config)
    watcher.run()


if __name__ == '__main__':
    main()
