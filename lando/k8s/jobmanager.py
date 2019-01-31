from lando.k8s.cluster import BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, EmptyDirVolume, Container, FieldRefEnvVar, AccessModes
import json
import os

DDSCLIENT_CONFIG_MOUNT_PATH = "/etc/ddsclient"
TMP_VOLUME_SIZE_IN_G = 1
BESPIN_JOB_LABEL_VALUE = "true"
WAIT_THEN_CAT_FILE = "while [ ! -f {} ]; do sleep 1; done; cat {}; echo ''"


class JobLabels(object):
    BESPIN_JOB = "bespin-job"  # expected value is "true"
    JOB_ID = "bespin-job-id"
    STEP_TYPE = "bespin-job-step"


class JobStepTypes(object):
    STAGE_DATA = "stage_data"
    RUN_WORKFLOW = "run_workflow"
    ORGANIZE_OUTPUT = "organize_output"
    SAVE_OUTPUT = "save_output"


class JobManager(object):
    def __init__(self, cluster_api, config, job):
        self.cluster_api = cluster_api
        self.config = config
        self.job = job
        self.names = Names(job)
        self.storage_class_name = config.storage_class_name
        self.default_metadata_labels = {
            JobLabels.BESPIN_JOB: BESPIN_JOB_LABEL_VALUE,
            JobLabels.JOB_ID: str(self.job.id),
        }
        self.label_selector = '{}={}'.format(JobLabels.BESPIN_JOB, BESPIN_JOB_LABEL_VALUE)

    def make_job_labels(self, job_step_type):
        labels = dict(self.default_metadata_labels)
        labels[JobLabels.STEP_TYPE] = job_step_type
        return labels

    def create_job_data_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.job_data,
            storage_size_in_g=self.job.volume_size,
            storage_class_name=self.storage_class_name,
            labels=self.default_metadata_labels,
        )

    def create_output_data_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.output_data,
            storage_size_in_g=self.job.volume_size,
            storage_class_name=self.storage_class_name,
            labels=self.default_metadata_labels,
        )

    def create_tmpout_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.tmpout,
            storage_size_in_g=self.job.volume_size,
            storage_class_name=self.storage_class_name,
            labels=self.default_metadata_labels,
        )

    def create_tmp_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.tmp,
            storage_size_in_g=TMP_VOLUME_SIZE_IN_G,
            storage_class_name=self.storage_class_name,
            labels=self.default_metadata_labels,
        )

    def create_stage_data_persistent_volumes(self):
        self.create_job_data_persistent_volume()

    def create_stage_data_job(self, input_files):
        stage_data_config = StageDataConfig(self.job, self.config)
        self._create_stage_data_config_map(name=self.names.stage_data,
                                           filename=stage_data_config.filename,
                                           workflow_url=self.job.workflow.url,
                                           job_order=self.job.workflow.job_order,
                                           input_files=input_files)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=False),
            ConfigMapVolume(self.names.stage_data,
                            mount_path=Paths.CONFIG_DIR,
                            config_map_name=self.names.stage_data,
                            source_key=stage_data_config.filename,
                            source_path=stage_data_config.filename),
            SecretVolume(self.names.data_store_secret,
                         mount_path=stage_data_config.data_store_secret_path,
                         secret_name=stage_data_config.data_store_secret_name),
        ]
        container = Container(
            name=self.names.stage_data,
            image_name=stage_data_config.image_name,
            command=stage_data_config.command,
            args=[stage_data_config.path],
            env_dict=stage_data_config.env_dict,
            requested_cpu=stage_data_config.requested_cpu,
            requested_memory=stage_data_config.requested_memory,
            volumes=volumes)
        labels = self.make_job_labels(JobStepTypes.STAGE_DATA)
        job_spec = BatchJobSpec(self.names.stage_data,
                                container=container,
                                labels=labels)
        return self.cluster_api.create_job(self.names.stage_data, job_spec, labels=labels)

    def _create_stage_data_config_map(self, name, filename, workflow_url, job_order, input_files):
        items = [
            self._stage_data_config_item("url", workflow_url, self.names.workflow_path),
            self._stage_data_config_item("write", job_order, self.names.job_order_path),
        ]
        for dds_file in input_files.dds_files:
            dest = '{}/{}'.format(Paths.JOB_DATA, dds_file.destination_path)
            items.append(self._stage_data_config_item("DukeDS", dds_file.file_id, dest))
        config_data = {"items": items}
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload, labels=self.default_metadata_labels)

    @staticmethod
    def _stage_data_config_item(type, source, dest):
        return {"type": type, "source": source, "dest": dest}

    def cleanup_stage_data_job(self):
        self.cluster_api.delete_job(self.names.stage_data)
        self.cluster_api.delete_config_map(self.names.stage_data)

    def create_run_workflow_persistent_volumes(self):
        self.create_tmpout_persistent_volume()
        self.create_output_data_persistent_volume()
        self.create_tmp_persistent_volume()

    def create_run_workflow_job(self):
        run_workflow_config = RunWorkflowConfig(self.job, self.config)
        system_data_volume = run_workflow_config.system_data_volume
        volumes = [
            PersistentClaimVolume(self.names.tmp,
                                  mount_path=Paths.TMP,
                                  volume_claim_name=self.names.tmp,
                                  read_only=False),
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=False),
            PersistentClaimVolume(self.names.tmpout,
                                  mount_path=Paths.TMPOUT_DATA,
                                  volume_claim_name=self.names.tmpout,
                                  read_only=False),
        ]
        if system_data_volume:
            volumes.append(PersistentClaimVolume(
                self.names.system_data,
                mount_path=system_data_volume.mount_path,
                volume_claim_name=system_data_volume.volume_claim_name,
                read_only=True))
        command_parts = run_workflow_config.command
        command_parts.extend(["--tmp-outdir-prefix", Paths.TMPOUT_DATA + "/",
                              "--outdir", Paths.OUTPUT_RESULTS_DIR + "/"])
        command_parts.extend([
            self.names.workflow_path,
            self.names.job_order_path,
            ">{}".format(self.names.run_workflow_stdout_path),
            "2>{}".format(self.names.run_workflow_stderr_path),
        ])
        container = Container(
            name=self.names.run_workflow,
            image_name=run_workflow_config.image_name,
            command=["bash", "-c", ' '.join(command_parts)],
            env_dict={
                "CALRISSIAN_POD_NAME": FieldRefEnvVar(field_path="metadata.name")
            },
            requested_cpu=run_workflow_config.requested_cpu,
            requested_memory=run_workflow_config.requested_memory,
            volumes=volumes
        )
        labels = self.make_job_labels(JobStepTypes.RUN_WORKFLOW)
        job_spec = BatchJobSpec(self.names.run_workflow,
                                container=container,
                                labels=labels)
        return self.cluster_api.create_job(self.names.run_workflow, job_spec, labels=labels)

    def cleanup_run_workflow_job(self):
        self.cluster_api.delete_job(self.names.run_workflow)
        self.cluster_api.delete_persistent_volume_claim(self.names.tmpout)
        self.cluster_api.delete_persistent_volume_claim(self.names.tmp)

    def create_organize_output_project_job(self):
        organize_output_config = OrganizeOutputConfig(self.job, self.config)
        self._create_organize_output_config_map(name=self.names.organize_output,
                                                filename=organize_output_config.filename,)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=False),
            ConfigMapVolume(self.names.organize_output,
                            mount_path=Paths.CONFIG_DIR,
                            config_map_name=self.names.organize_output,
                            source_key=organize_output_config.filename,
                            source_path=organize_output_config.filename),
        ]
        container = Container(
            name=self.names.organize_output,
            image_name=organize_output_config.image_name,
            command=organize_output_config.command,
            args=[organize_output_config.path],
            requested_cpu=organize_output_config.requested_cpu,
            requested_memory=organize_output_config.requested_memory,
            volumes=volumes)
        labels = self.make_job_labels(JobStepTypes.ORGANIZE_OUTPUT)
        job_spec = BatchJobSpec(self.names.organize_output,
                                container=container,
                                labels=labels)
        return self.cluster_api.create_job(self.names.organize_output, job_spec, labels=labels)

    def _create_organize_output_config_map(self, name, filename):
        config_data = {
            "destination_dir": Paths.OUTPUT_RESULTS_DIR,
            "workflow_path": self.names.workflow_path,
            "job_order_path": self.names.job_order_path,
            "job_data_path": "TODO",
            "cwltool_stdout_path": self.names.run_workflow_stdout_path,
            "cwltool_stderr_path": self.names.run_workflow_stderr_path,
            "methods_template": "TODO",
        }
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload, labels=self.default_metadata_labels)

    def cleanup_organize_output_project_job(self):
        self.cluster_api.delete_job(self.names.organize_output)

    def create_save_output_job(self, share_dds_ids):
        save_output_config = SaveOutputConfig(self.job, self.config)
        self._create_save_output_config_map(name=self.names.save_output,
                                            filename=save_output_config.filename,
                                            share_dds_ids=share_dds_ids)
        sidecar_volume = EmptyDirVolume(self.names.save_output_sidecar_volume, mount_path=Paths.SIDECAR_OUTPUT_DIR)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=True),
            ConfigMapVolume(self.names.stage_data,
                            mount_path=Paths.CONFIG_DIR,
                            config_map_name=self.names.save_output,
                            source_key=save_output_config.filename,
                            source_path=save_output_config.filename),
            SecretVolume(self.names.data_store_secret,
                         mount_path=save_output_config.data_store_secret_path,
                         secret_name=save_output_config.data_store_secret_name),
            sidecar_volume,
        ]
        container = Container(
            name=self.names.save_output,
            image_name=save_output_config.image_name,
            command=save_output_config.command,
            args=[save_output_config.path, save_output_config.sidecar_filepath],
            working_dir=Paths.OUTPUT_RESULTS_DIR,
            env_dict=save_output_config.env_dict,
            requested_cpu=save_output_config.requested_cpu,
            requested_memory=save_output_config.requested_memory,
            volumes=volumes)
        save_output_container = Container(
            name=self.names.save_output_sidecar,
            image_name=save_output_config.sidecar_image_name,
            command=save_output_config.sidecar_command,
            volumes=[sidecar_volume]
        )
        labels = self.make_job_labels(JobStepTypes.SAVE_OUTPUT)
        job_spec = BatchJobSpec(self.names.save_output,
                                container=container,
                                additional_containers=[save_output_container],
                                labels=labels)
        return self.cluster_api.create_job(self.names.save_output, job_spec, labels=labels)

    def _create_save_output_config_map(self, name, filename, share_dds_ids):
        config_data = {
            "destination": self.names.output_project_name,
            "paths": [Paths.OUTPUT_DATA],
            "share": {
                "dds_user_ids": share_dds_ids
            }
        }
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload, labels=self.default_metadata_labels)

    def read_save_output_pod_logs(self):
        # TODO what if there are failed pods and on that finally succeeds?
        # TODO Need to filter by job id as well
        save_output_pod_selector = '{}={}'.format(JobLabels.STEP_TYPE, JobStepTypes.SAVE_OUTPUT)
        pods = self.cluster_api.list_pods(label_selector=save_output_pod_selector)
        pod = pods[0]
        return self.cluster_api.read_pod_logs(pod.metadata.name, container=self.names.save_output_sidecar)

    def cleanup_save_output_job(self):
        self.cluster_api.delete_job(self.names.save_output)
        self.cluster_api.delete_config_map(self.names.save_output)
        self.cluster_api.delete_persistent_volume_claim(self.names.job_data)
        self.cluster_api.delete_persistent_volume_claim(self.names.output_data)

    def cleanup_all(self):
        self.cleanup_jobs_and_config_maps()

        # Delete all PVC
        for pvc in self.cluster_api.list_persistent_volume_claims(label_selector=self.label_selector):
            self.cluster_api.delete_persistent_volume_claim(pvc.metadata.name)

    def cleanup_jobs_and_config_maps(self):
        # Delete all Jobs
        for job in self.cluster_api.list_jobs(label_selector=self.label_selector):
            self.cluster_api.delete_job(job.metadata.name)

        # Delete all config maps
        for config_map in self.cluster_api.list_config_maps(label_selector=self.label_selector):
            self.cluster_api.delete_config_map(config_map.metadata.name)


class Names(object):
    def __init__(self, job):
        job_id = job.id
        suffix = '{}-{}'.format(job.id, job.username)
        # Volumes
        self.job_data = 'job-data-{}'.format(suffix)
        self.output_data = 'output-data-{}'.format(suffix)
        self.tmpout = 'tmpout-{}'.format(suffix)
        self.tmp = 'tmp-{}'.format(suffix)

        # Job Names
        self.stage_data = 'stage-data-{}'.format(suffix)
        self.run_workflow = 'run-workflow-{}'.format(suffix)
        self.organize_output = 'organize-output-{}'.format(suffix)
        self.save_output = 'save-output-{}'.format(suffix)

        self.save_output_sidecar = 'save-output-{}-sidecar'.format(suffix)
        self.save_output_sidecar_volume = 'save-output-{}-sidecar-volume'.format(suffix)
        self.user_data = 'user-data-{}'.format(suffix)
        self.data_store_secret = 'data-store-{}'.format(suffix)
        self.output_project_name = 'Bespin-job-{}-results'.format(job_id)
        self.workflow_path = '{}/{}'.format(Paths.WORKFLOW, os.path.basename(job.workflow.url))
        self.job_order_path = '{}/job-order.json'.format(Paths.JOB_DATA)
        self.system_data = 'system-data-{}'.format(suffix)
        self.run_workflow_stdout_path = '{}/cwltool-output.json'.format(Paths.OUTPUT_DATA)
        self.run_workflow_stderr_path = '{}/cwltool-output.log'.format(Paths.OUTPUT_DATA)


class Paths(object):
    JOB_DATA = '/bespin/job-data'
    WORKFLOW = '/bespin/job-data/workflow'
    CONFIG_DIR = '/bespin/config'
    STAGE_DATA_CONFIG_FILE = '/bespin/config/stagedata.json'
    OUTPUT_DATA = '/bespin/output-data'
    OUTPUT_RESULTS_DIR = '/bespin/output-data/results'
    TMPOUT_DATA = '/bespin/tmpout'
    TMP = '/tmp'
    SIDECAR_OUTPUT_DIR = '/sidecar'


class StageDataConfig(object):
    def __init__(self, job, config):
        # job parameter is not used but is here to allow future customization based on job
        self.filename = "stagedata.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        self.data_store_secret_name = config.data_store_settings.secret_name
        self.data_store_secret_path = DDSCLIENT_CONFIG_MOUNT_PATH
        self.env_dict = {"DDSCLIENT_CONF": "{}/config".format(DDSCLIENT_CONFIG_MOUNT_PATH)}

        stage_data_settings = config.stage_data_settings
        self.image_name = stage_data_settings.image_name
        self.command = stage_data_settings.command
        self.requested_cpu = stage_data_settings.requested_cpu
        self.requested_memory = stage_data_settings.requested_memory


class RunWorkflowConfig(object):
    def __init__(self, job, config):
        self.image_name = job.vm_settings.image_name
        self.command = job.vm_settings.cwl_commands.base_command

        run_workflow_settings = config.run_workflow_settings
        self.requested_cpu = run_workflow_settings.requested_cpu
        self.requested_memory = run_workflow_settings.requested_memory
        self.system_data_volume = run_workflow_settings.system_data_volume


class OrganizeOutputConfig(object):
    def __init__(self, job, config):
        self.filename = "organizeoutput.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        # job parameter is not used but is here to allow future customization based on job
        organize_output_settings = config.organize_output_settings
        self.image_name = organize_output_settings.image_name
        self.command = organize_output_settings.command
        self.requested_cpu = organize_output_settings.requested_cpu
        self.requested_memory = organize_output_settings.requested_memory


class SaveOutputConfig(object):
    def __init__(self, job, config):
        # job parameter is not used but is here to allow future customization based on job
        self.filename = "saveoutput.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        self.data_store_secret_name = config.data_store_settings.secret_name
        self.data_store_secret_path = DDSCLIENT_CONFIG_MOUNT_PATH
        self.env_dict = {"DDSCLIENT_CONF": "{}/config".format(DDSCLIENT_CONFIG_MOUNT_PATH)}

        self.sidecar_filepath = "{}/results.json".format(Paths.SIDECAR_OUTPUT_DIR)
        self.sidecar_image_name = "busybox"
        self.sidecar_command = [
            "sh", "-c",
            WAIT_THEN_CAT_FILE.format(self.sidecar_filepath, self.sidecar_filepath)
        ]

        save_output_settings = config.save_output_settings
        self.image_name = save_output_settings.image_name
        self.command = save_output_settings.command
        self.requested_cpu = save_output_settings.requested_cpu
        self.requested_memory = save_output_settings.requested_memory
