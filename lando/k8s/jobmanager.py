from lando.k8s.cluster import BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, FieldRefEnvVar, AccessModes
import json
import os

DDSCLIENT_CONFIG_MOUNT_PATH = "/etc/ddsclient"


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
    def __init__(self, cluster_api, job_settings, job):
        self.cluster_api = cluster_api
        self.job_settings = job_settings
        self.job = job
        self.names = Names(job)
        self.storage_class_name = job_settings.config.storage_class_name

    def make_job_labels(self, job_step_type):
        return {
            JobLabels.BESPIN_JOB: "true",
            JobLabels.JOB_ID: str(self.job.id),
            JobLabels.STEP_TYPE: job_step_type
        }

    def create_job_data_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.job_data,
            storage_size_in_g=self.job.volume_size,  # TODO better calculate this
            storage_class_name=self.storage_class_name
        )

    def create_output_data_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.output_data,
            storage_size_in_g=self.job.volume_size,  # TODO better calculate this
            storage_class_name=self.storage_class_name
        )

    def create_tmpout_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.tmpout,
            storage_size_in_g=self.job.volume_size,  # TODO better calculate this
            storage_class_name=self.storage_class_name
        )

    def create_tmp_persistent_volume(self):
        self.cluster_api.create_persistent_volume_claim(
            self.names.tmp,
            storage_size_in_g=1,  # TODO better calculate this
            storage_class_name=self.storage_class_name
        )

    def create_stage_data_persistent_volumes(self):
        self.create_job_data_persistent_volume()

    def create_stage_data_job(self, input_files):
        stage_data_config = StageDataConfig(self.job, self.job_settings)
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
        job_spec = BatchJobSpec(self.names.stage_data,
                                container=container,
                                labels=self.make_job_labels(JobStepTypes.STAGE_DATA))
        return self.cluster_api.create_job(self.names.stage_data, job_spec)

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
        self.cluster_api.create_config_map(name=name, data=payload)

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
        run_workflow_config = RunWorkflowConfig(self.job, self.job_settings)
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
                              "--outdir", Paths.OUTPUT_DATA + "/"])
        command_parts.extend([self.names.workflow_path, self.names.job_order_path])
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
        job_spec = BatchJobSpec(self.names.run_workflow,
                                container=container,
                                labels=self.make_job_labels(JobStepTypes.RUN_WORKFLOW))
        return self.cluster_api.create_job(self.names.run_workflow, job_spec)

    def cleanup_run_workflow_job(self):
        self.cluster_api.delete_job(self.names.run_workflow)
        self.cluster_api.delete_persistent_volume_claim(self.names.tmpout)
        self.cluster_api.delete_persistent_volume_claim(self.names.tmp)

    def create_organize_output_project_job(self):
        organize_output_config = OrganizeOutputConfig(self.job, self.job_settings)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=False),
        ]
        container = Container(
            name=self.names.organize_output,
            image_name=organize_output_config.image_name,
            command=organize_output_config.command,
            requested_cpu=organize_output_config.requested_cpu,
            requested_memory=organize_output_config.requested_memory,
            volumes=volumes)
        job_spec = BatchJobSpec(self.names.organize_output,
                                container=container,
                                labels=self.make_job_labels(JobStepTypes.ORGANIZE_OUTPUT))
        return self.cluster_api.create_job(self.names.organize_output, job_spec)

    def cleanup_organize_output_project_job(self):
        self.cluster_api.delete_job(self.names.organize_output)

    def create_save_output_job(self):
        save_output_config = SaveOutputConfig(self.job, self.job_settings)
        self._create_save_output_config_map(name=self.names.save_output,
                                            filename=save_output_config.filename)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            ConfigMapVolume(self.names.stage_data,
                            mount_path=Paths.CONFIG_DIR,
                            config_map_name=self.names.save_output,
                            source_key=save_output_config.filename,
                            source_path=save_output_config.filename),
            SecretVolume(self.names.data_store_secret,
                         mount_path=save_output_config.data_store_secret_path,
                         secret_name=save_output_config.data_store_secret_name),
        ]
        container = Container(
            name=self.names.save_output,
            image_name=save_output_config.image_name,
            command=save_output_config.command,
            args=[save_output_config.path],
            working_dir=Paths.OUTPUT_DATA,
            env_dict=save_output_config.env_dict,
            requested_cpu=save_output_config.requested_cpu,
            requested_memory=save_output_config.requested_memory,
            volumes=volumes)
        job_spec = BatchJobSpec(self.names.save_output,
                                container=container,
                                labels=self.make_job_labels(JobStepTypes.SAVE_OUTPUT))
        return self.cluster_api.create_job(self.names.save_output, job_spec)

    def _create_save_output_config_map(self, name, filename):
        config_data = {
            "destination": self.names.output_project_name,
            "paths": [Paths.OUTPUT_DATA]
        }
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload)

    def cleanup_save_output_job(self):
        self.cluster_api.delete_job(self.names.save_output)
        self.cluster_api.delete_config_map(self.names.save_output)
        self.cluster_api.delete_persistent_volume_claim(self.names.job_data)
        self.cluster_api.delete_persistent_volume_claim(self.names.output_data)


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

        self.user_data = 'user-data-{}'.format(suffix)
        self.data_store_secret = 'data-store-{}'.format(suffix)
        self.output_project_name = 'Bespin-job-{}-results'.format(job_id)
        self.workflow_path = '{}/{}'.format(Paths.WORKFLOW, os.path.basename(job.workflow.url))
        self.job_order_path = '{}/job-order.json'.format(Paths.JOB_DATA)
        self.system_data = 'system-data-{}'.format(suffix)

class Paths(object):
    JOB_DATA = '/bespin/job-data'
    WORKFLOW = '/bespin/job-data/workflow'
    CONFIG_DIR = '/bespin/config'
    STAGE_DATA_CONFIG_FILE = '/bespin/config/stagedata.json'
    OUTPUT_DATA = '/bespin/output-data'
    TMPOUT_DATA = '/bespin/tmpout'
    TMP = '/tmp'


class StageDataConfig(object):
    def __init__(self, job, job_settings):
        config = job_settings.config
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
    def __init__(self, job, job_settings):
        self.image_name = job.vm_settings.image_name
        self.command = job.vm_settings.cwl_commands.base_command

        run_workflow_settings = job_settings.config.run_workflow_settings
        self.requested_cpu = run_workflow_settings.requested_cpu
        self.requested_memory = run_workflow_settings.requested_memory
        self.system_data_volume = run_workflow_settings.system_data_volume


class OrganizeOutputConfig(object):
    def __init__(self, job, job_settings):
        config = job_settings.config

        organize_output_settings = config.organize_output_settings
        self.image_name = organize_output_settings.image_name
        self.command = organize_output_settings.command
        self.requested_cpu = organize_output_settings.requested_cpu
        self.requested_memory = organize_output_settings.requested_memory


class SaveOutputConfig(object):
    def __init__(self, job, job_settings):
        config = job_settings.config
        self.filename = "saveoutput.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        self.data_store_secret_name = config.data_store_settings.secret_name
        self.data_store_secret_path = DDSCLIENT_CONFIG_MOUNT_PATH
        self.env_dict = {"DDSCLIENT_CONF": "{}/config".format(DDSCLIENT_CONFIG_MOUNT_PATH)}

        save_output_settings = config.save_output_settings
        self.image_name = save_output_settings.image_name
        self.command = save_output_settings.command
        self.requested_cpu = save_output_settings.requested_cpu
        self.requested_memory = save_output_settings.requested_memory
