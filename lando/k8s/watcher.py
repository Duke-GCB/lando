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
    JobStepTypes.SAVE_OUTPUT: JobCommands.STORE_JOB_OUTPUT_COMPLETE
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
        return ClusterApi(settings.host, settings.token, settings.namespace, incluster_config=False,
                          verify_ssl=False)  # TODO REMOVE THIS

    def run(self):
        # TODO filtering by a label
        self.cluster_api.wait_for_job_events(self.on_job_change)

    def on_job_change(self, job):
        logging.info("Received job {}".format(job.metadata.name))
        if check_condition_status(job, JobConditionType.COMPLETE):
            self.on_job_succeeded(job)
        elif check_condition_status(job, JobConditionType.FAILED):
            self.on_job_failed(job)

    def on_job_succeeded(self, job):
        bespin_job_id = job.metadata.labels.get(JobLabels.JOB_ID)
        bespin_job_step = job.metadata.labels.get(JobLabels.STEP_TYPE)
        if bespin_job_id and bespin_job_step:
            self.send_step_complete_message(bespin_job_step, bespin_job_id, bespin_job_step)
            if bespin_job_step == JobStepTypes.STAGE_DATA:
                payload = JobStepPayload(bespin_job_id, None, JobCommands.STAGE_JOB_COMPLETE)
                self.lando_client.job_step_complete(payload)
            elif bespin_job_step == JobStepTypes.RUN_WORKFLOW:
                payload = JobStepPayload(bespin_job_id, None, JobCommands.RUN_JOB_COMPLETE)
                self.lando_client.job_step_complete(payload)
            else:
                print("TODO", bespin_job_step, bespin_job_id)

    def send_step_complete_message(self, bespin_job_step, bespin_job_id, step_type):
        job_command = COMPLETE_JOB_STEP_TO_COMMAND.get(bespin_job_step)
        if job_command:
            payload = JobStepPayload(bespin_job_id, None, job_command)
            if job_command == JobCommands.STORE_JOB_OUTPUT_COMPLETE:
                self.lando_client.job_step_store_output_complete(payload, None)
            else:
                self.lando_client.job_step_complete(payload)
        else:
            print("TODO", bespin_job_step, bespin_job_id, step_type)

    def on_job_failed(self, job):
        print("Failed", job.metadata.name)
        print("job id", job.metadata.labels.get(JobLabels.JOB_ID))
        print("step type", job.metadata.labels.get(JobLabels.STEP_TYPE))


def main():
    config = create_server_config(sys.argv[1])
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    watcher = JobWatcher(config)
    watcher.run()


if __name__ == '__main__':
    main()
