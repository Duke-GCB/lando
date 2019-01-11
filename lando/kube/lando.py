import os
import logging
import json
from lando.server.lando import Lando, JobApi, WorkProgressQueue, WORK_PROGRESS_EXCHANGE_NAME, JobStates
from lando.kube.cluster import ClusterApi, BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, SecretEnvVar


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
        cluster_api = self.config.cluster_api
        return ClusterApi(cluster_api.host, cluster_api.token, cluster_api.namespace,
                          incluster_config=cluster_api.incluster_config,
                          verify_ssl=False)  # TODO REMOVE THIS


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
        self.job_name = "job-{}".format(self.job_id)
        self.volume_name = "job-{}-volume".format(self.job_id)
        self.bespin_api_secret_name = 'bespin-api-admin'
        self.cluster_api_secret_name = 'cluster-api'
        self.rabbit_users_secret_name = 'rabbit-users'

    def start_job(self, payload):
        job = self.job_api.get_job()
        #self._create_bespin_api_key_secret()
        #self._create_cluster_api_secret()
        self._create_volume(job)
        self._create_job(job)

    def _create_bespin_api_key_secret(self):
        secret_config = {
            "url": self.config.bespin_api_settings.url,
            "token": self.config.bespin_api_settings.token
        }
        secret = self.cluster_api.create_secret(
            name=self.bespin_api_secret_name,
            string_value_dict=secret_config
        )
        logging.info("Created secret {}".format(secret))

    def _create_cluster_api_secret(self):
        secret_config = {
            "host": self.config.cluster_api.host,
            "token": self.config.cluster_api.token,
            "namespace": self.config.cluster_api.namespace
        }
        secret = self.cluster_api.create_secret(
            name=self.cluster_api_secret_name,
            string_value_dict=secret_config
        )
        logging.info("Created secret {}".format(secret))

    def _create_volume(self, job):
        volume_claim = self.cluster_api.create_persistent_volume_claim(
            name=self.volume_name,
            storage_size_in_g=job.volume_size,
            storage_class_name=None,
        )
        logging.info("Created volume claim {}".format(volume_claim))

    def _create_job(self, job):
        persistent_claim_volume = PersistentClaimVolume(self.volume_name,
                                                        mount_path="/data",
                                                        volume_claim_name=self.volume_name)
        container = Container(
            name=self.job_name,
            image_name=job.vm_settings.image_name,
            command="lando_kube_worker",
            args=[],
            working_dir="/data",
            env_dict={
                "JOB_ID": self.job_id,
                "WORKFLOW_DIR": "/data",
                "BESPIN_API_URL": SecretEnvVar(name=self.bespin_api_secret_name, key='url'),
                "BESPIN_API_TOKEN": SecretEnvVar(name=self.bespin_api_secret_name, key='token'),
                "BESPIN_CLUSTER_HOST": SecretEnvVar(name=self.cluster_api_secret_name, key='host'),
                "BESPIN_CLUSTER_TOKEN": SecretEnvVar(name=self.cluster_api_secret_name, key='token'),
                "BESPIN_CLUSTER_NAMESPACE": SecretEnvVar(name=self.cluster_api_secret_name, key='namespace'),
                "BESPIN_INCLUSTER_CONFIG": SecretEnvVar(name=self.cluster_api_secret_name, key='incluster_config'),
                "BESPIN_RABBIT_HOST": os.environ["BESPIN_RABBIT_HOST"],
                "BESPIN_QUEUE_LANDO_USERNAME": SecretEnvVar(self.rabbit_users_secret_name, key='LANDO_USERNAME'),
                "BESPIN_QUEUE_LANDO_PASSWORD": SecretEnvVar(self.rabbit_users_secret_name, key='LANDO_PASSWORD'),
                "BESPIN_QUEUE_WORKER_USERNAME": SecretEnvVar(self.rabbit_users_secret_name, key='WORKER_USERNAME'),
                "BESPIN_QUEUE_WORKER_PASSWORD": SecretEnvVar(self.rabbit_users_secret_name, key='WORKER_PASSWORD'),
                "BESPIN_RABBIT_QUEUE": os.environ["BESPIN_RABBIT_QUEUE"],
            },
            requested_cpu="100m",
            requested_memory="64Mi",
            volumes=[
                persistent_claim_volume,
            ],
        )
        job_spec = BatchJobSpec(self.job_name, container=container)
        job = self.cluster_api.create_job(self.job_name, job_spec)
        logging.info("Created job {}".format(job))

    def restart_job(self, payload):
        logging.error("Restart job {}".format(payload))

    def cancel_job(self, payload):
        logging.error("Cancel job {}".format(payload))

    def _log_error(self, message):
        job = self.job_api.get_job()
        self.job_api.save_error_details(job.step, message)

    def _set_job_state(self, state):
        self.job_api.set_job_state(state)
        self._send_job_progress_notification()

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

