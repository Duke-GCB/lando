from lando.k8s.cluster import BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, FieldRefEnvVar
import json
import os
import re
import dateutil


DDSCLIENT_CONFIG_MOUNT_PATH = "/etc/ddsclient"
BESPIN_JOB_LABEL_VALUE = "true"


class JobLabels(object):
    BESPIN_JOB = "bespin-job"  # expected value is "true"
    JOB_ID = "bespin-job-id"
    STEP_TYPE = "bespin-job-step"


class JobStepTypes(object):
    STAGE_DATA = "stage_data"
    RUN_WORKFLOW = "run_workflow"
    ORGANIZE_OUTPUT = "organize_output"
    SAVE_OUTPUT = "save_output"
    RECORD_OUTPUT_PROJECT = "record_output_project"


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
        label_ary = ['{}={}'.format(k,v) for k,v in self.default_metadata_labels.items()]
        self.label_selector = ','.join(label_ary)

    def make_job_labels(self, job_step_type):
        labels = dict(self.default_metadata_labels)
        labels[JobLabels.STEP_TYPE] = job_step_type
        return labels

    def create_job_data_persistent_volume(self, stage_data_size_in_g):
        self.cluster_api.create_persistent_volume_claim(
            self.names.job_data,
            storage_size_in_g=stage_data_size_in_g,
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

    def create_stage_data_persistent_volumes(self, stage_data_size_in_g):
        self.create_job_data_persistent_volume(stage_data_size_in_g)

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
            args=[stage_data_config.path, self.names.workflow_input_files_metadata_path],
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
        self.create_output_data_persistent_volume()

    def create_run_workflow_job(self):
        run_workflow_config = RunWorkflowConfig(self.job, self.config)
        system_data_volume = run_workflow_config.system_data_volume
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
        if system_data_volume:
            volumes.append(PersistentClaimVolume(
                self.names.system_data,
                mount_path=system_data_volume.mount_path,
                volume_claim_name=system_data_volume.volume_claim_name,
                read_only=True))
        command = run_workflow_config.command
        command.extend(["--tmp-outdir-prefix", Paths.TMPOUT_DATA + "/",
                        "--outdir", Paths.OUTPUT_RESULTS_DIR + "/",
                        "--max-ram", self.job.job_flavor_memory,
                        "--max-cores", str(self.job.job_flavor_cpus),
                        "--usage-report", self.names.usage_report_path,
                        "--stdout", self.names.run_workflow_stdout_path,
                        "--stderr", self.names.run_workflow_stderr_path,
                        self.names.workflow_path,
                        self.names.job_order_path,
                        ])
        container = Container(
            name=self.names.run_workflow,
            image_name=run_workflow_config.image_name,
            command=command,
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

    def create_organize_output_project_job(self, methods_document_content):
        organize_output_config = OrganizeOutputConfig(self.job, self.config)
        self._create_organize_output_config_map(name=self.names.organize_output,
                                                filename=organize_output_config.filename,
                                                methods_document_content=methods_document_content)
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

    def _create_organize_output_config_map(self, name, filename, methods_document_content):
        config_data = {
            "bespin_job_id": self.job.id,
            "destination_dir": Paths.OUTPUT_RESULTS_DIR,
            "workflow_path": self.names.workflow_path,
            "job_order_path": self.names.job_order_path,
            "bespin_workflow_stdout_path": self.names.run_workflow_stdout_path,
            "bespin_workflow_stderr_path": self.names.run_workflow_stderr_path,
            "methods_template": methods_document_content,
            "additional_log_files": [
                self.names.usage_report_path
            ]
        }
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload, labels=self.default_metadata_labels)

    def cleanup_organize_output_project_job(self):
        self.cluster_api.delete_config_map(self.names.organize_output)
        self.cluster_api.delete_job(self.names.organize_output)

    def create_save_output_job(self, share_dds_ids):
        save_output_config = SaveOutputConfig(self.job, self.config)
        self._create_save_output_config_map(name=self.names.save_output,
                                            filename=save_output_config.filename,
                                            share_dds_ids=share_dds_ids,
                                            activity_name=save_output_config.activity_name,
                                            activity_description=save_output_config.activity_description)
        volumes = [
            PersistentClaimVolume(self.names.job_data,
                                  mount_path=Paths.JOB_DATA,
                                  volume_claim_name=self.names.job_data,
                                  read_only=True),
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=False),  # writable so we can write project_details file
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
            args=[save_output_config.path, self.names.annotate_project_details_path],
            working_dir=Paths.OUTPUT_RESULTS_DIR,
            env_dict=save_output_config.env_dict,
            requested_cpu=save_output_config.requested_cpu,
            requested_memory=save_output_config.requested_memory,
            volumes=volumes)
        labels = self.make_job_labels(JobStepTypes.SAVE_OUTPUT)
        job_spec = BatchJobSpec(self.names.save_output,
                                container=container,
                                labels=labels)
        return self.cluster_api.create_job(self.names.save_output, job_spec, labels=labels)

    def _create_save_output_config_map(self, name, filename, share_dds_ids, activity_name, activity_description):
        config_data = {
            "destination": self.names.output_project_name,
            "readme_file_path": Paths.REMOTE_README_FILE_PATH,
            "paths": [Paths.OUTPUT_RESULTS_DIR],
            "share": {
                "dds_user_ids": share_dds_ids
            },
            "activity": {
                "name": activity_name,
                "description": activity_description,
                "started_on": "",
                "ended_on": "",
                "input_file_versions_json_path": self.names.workflow_input_files_metadata_path,
                "workflow_output_json_path": self.names.run_workflow_stdout_path
            }
        }
        payload = {
            filename: json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=name, data=payload, labels=self.default_metadata_labels)

    def cleanup_save_output_job(self):
        self.cluster_api.delete_job(self.names.save_output)
        self.cluster_api.delete_config_map(self.names.save_output)
        self.cluster_api.delete_persistent_volume_claim(self.names.job_data)

    def create_record_output_project_job(self):
        config = RecordOutputProjectConfig(self.job, self.config)
        volumes = [
            PersistentClaimVolume(self.names.output_data,
                                  mount_path=Paths.OUTPUT_DATA,
                                  volume_claim_name=self.names.output_data,
                                  read_only=True),
        ]
        container = Container(
            name=self.names.record_output_project,
            image_name=config.image_name,
            command=["sh"],
            args=[self.names.annotate_project_details_path],
            working_dir=Paths.OUTPUT_RESULTS_DIR,
            env_dict={"MY_POD_NAME": FieldRefEnvVar(field_path="metadata.name")},
            requested_cpu=config.requested_cpu,
            requested_memory=config.requested_memory,
            volumes=volumes)
        labels = self.make_job_labels(JobStepTypes.RECORD_OUTPUT_PROJECT)
        job_spec = BatchJobSpec(self.names.record_output_project,
                                container=container,
                                labels=labels,
                                service_account_name=config.service_account_name)
        return self.cluster_api.create_job(self.names.record_output_project, job_spec, labels=labels)

    def read_record_output_project_details(self):
        job_step_selector='{},{}={}'.format(self.label_selector,
                                            JobLabels.STEP_TYPE, JobStepTypes.RECORD_OUTPUT_PROJECT)
        pods = self.cluster_api.list_pods(label_selector=job_step_selector)
        if len(pods) != 1:
            raise ValueError("Incorrect number of pods for record output step: {}".format(len(pods)))
        annotations = pods[0].metadata.annotations
        project_id = annotations.get('project_id')
        if not project_id:
            raise ValueError("Missing project_id in pod annotations: {}".format(pods[0].metadata.name))
        readme_file_id = annotations.get('readme_file_id')
        if not readme_file_id:
            raise ValueError("Missing readme_file_id in pod annotations: {}".format(pods[0].metadata.name))
        return project_id, readme_file_id

    def cleanup_record_output_project_job(self):
        self.cluster_api.delete_job(self.names.record_output_project)
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
        stripped_username = re.sub(r'@.*', '', job.username)
        suffix = '{}-{}'.format(job.id, stripped_username)
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
        self.record_output_project = 'record-output-project-{}'.format(suffix)

        self.user_data = 'user-data-{}'.format(suffix)
        self.data_store_secret = 'data-store-{}'.format(suffix)
        job_created = dateutil.parser.parse(job.created).strftime("%Y-%m-%d")
        self.output_project_name = "Bespin {} v{} {} {}".format(
            job.workflow.name, job.workflow.version, job.name, job_created)
        self.workflow_path = '{}/{}'.format(Paths.WORKFLOW, os.path.basename(job.workflow.url))
        self.job_order_path = '{}/job-order.json'.format(Paths.JOB_DATA)
        self.workflow_input_files_metadata_path = '{}/workflow-input-files-metadata.json'.format(Paths.JOB_DATA)
        self.system_data = 'system-data-{}'.format(suffix)
        self.run_workflow_stdout_path = '{}/bespin-workflow-output.json'.format(Paths.OUTPUT_DATA)
        self.run_workflow_stderr_path = '{}/bespin-workflow-output.log'.format(Paths.OUTPUT_DATA)
        self.annotate_project_details_path = '{}/annotate_project_details.sh'.format(Paths.OUTPUT_DATA)
        self.usage_report_path = '{}/job-{}-resource-usage.json'.format(Paths.OUTPUT_DATA, suffix)


class Paths(object):
    JOB_DATA = '/bespin/job-data'
    WORKFLOW = '/bespin/job-data/workflow'
    CONFIG_DIR = '/bespin/config'
    STAGE_DATA_CONFIG_FILE = '/bespin/config/stagedata.json'
    OUTPUT_DATA = '/bespin/output-data'
    OUTPUT_RESULTS_DIR = '/bespin/output-data/results'
    TMPOUT_DATA = '/bespin/output-data/tmpout'
    REMOTE_README_FILE_PATH = 'results/docs/README.md'


class StageDataConfig(object):
    def __init__(self, job, config):
        self.filename = "stagedata.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        self.data_store_secret_name = config.data_store_settings.secret_name
        self.data_store_secret_path = DDSCLIENT_CONFIG_MOUNT_PATH
        self.env_dict = {"DDSCLIENT_CONF": "{}/config".format(DDSCLIENT_CONFIG_MOUNT_PATH)}

        job_stage_data_settings = job.k8s_settings.stage_data
        self.image_name = job_stage_data_settings.image_name
        self.command = job_stage_data_settings.base_command
        self.requested_cpu = job_stage_data_settings.cpus
        self.requested_memory = job_stage_data_settings.memory


class RunWorkflowConfig(object):
    def __init__(self, job, config):
        job_run_workflow_settings = job.k8s_settings.run_workflow
        self.image_name = job_run_workflow_settings.image_name
        self.command = job_run_workflow_settings.base_command
        self.requested_cpu = job_run_workflow_settings.cpus
        self.requested_memory = job_run_workflow_settings.memory

        run_workflow_settings = config.run_workflow_settings
        self.system_data_volume = run_workflow_settings.system_data_volume


class OrganizeOutputConfig(object):
    def __init__(self, job, config):
        self.filename = "organizeoutput.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)

        job_organize_output_settings = job.k8s_settings.organize_output
        self.image_name = job_organize_output_settings.image_name
        self.command = job_organize_output_settings.base_command
        self.requested_cpu = job_organize_output_settings.cpus
        self.requested_memory = job_organize_output_settings.memory


class SaveOutputConfig(object):
    def __init__(self, job, config):
        self.filename = "saveoutput.json"
        self.path = '{}/{}'.format(Paths.CONFIG_DIR, self.filename)
        self.data_store_secret_name = config.data_store_settings.secret_name
        self.data_store_secret_path = DDSCLIENT_CONFIG_MOUNT_PATH
        self.env_dict = {"DDSCLIENT_CONF": "{}/config".format(DDSCLIENT_CONFIG_MOUNT_PATH)}

        job_save_output_settings = job.k8s_settings.save_output
        self.image_name = job_save_output_settings.image_name
        self.command = job_save_output_settings.base_command
        self.requested_cpu = job_save_output_settings.cpus
        self.requested_memory = job_save_output_settings.memory

        self.activity_name = "{} - Bespin Job {}".format(job.name, job.id)
        self.activity_description = "Bespin Job {} - Workflow {} v{}".format(
            job.id, job.workflow.name, job.workflow.version)


class RecordOutputProjectConfig(object):
    def __init__(self, job, config):
        job_record_output_project_settings = job.k8s_settings.record_output_project

        record_output_project_settings = config.record_output_project_settings
        self.image_name = job_record_output_project_settings.image_name
        self.requested_cpu = job_record_output_project_settings.cpus
        self.requested_memory = job_record_output_project_settings.memory
        self.service_account_name = record_output_project_settings.service_account_name
        self.project_id_fieldname = 'project_id'
        self.readme_file_id_fieldname = 'readme_file_id'
