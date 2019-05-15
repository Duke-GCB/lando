import os
import json
import subprocess
from ddsc.config import LOCAL_CONFIG_ENV as DDSCLIENT_CONFIG_ENV, Config as DukeDSConfig

DDSCLIENT_CONFIG_ENV_NAME = "DDSCLIENT_CONF"
DDSCLIENT_CONFIG_URL = 'url'
DDSCLIENT_CONFIG_USER_KEY = 'user_key'
DDSCLIENT_CONFIG_AGENT_KEY = 'agent_key'


class StageDataTypes(object):
    URL = "url"
    WRITE = "write"
    DUKEDS = "DukeDS"


class WorkflowTypes(object):
    ZIPPED = 'zipped'
    PACKED = 'packed'


class BaseCommand(object):
    def __init__(self):
        pass

    @staticmethod
    def write_json_file(filename, data):
        with open(filename, 'w') as outfile:
            outfile.write(json.dumps(data))

    @staticmethod
    def dds_config_dict(credentials):
        return {
            DukeDSConfig.URL: credentials.endpoint_api_root,
            DukeDSConfig.AGENT_KEY: credentials.endpoint_agent_key,
            DukeDSConfig.USER_KEY: credentials.token,
        }

    def write_dds_config_file(self, dds_config_filename, dds_credentials):
        self.write_json_file(dds_config_filename, self.dds_config_dict(dds_credentials))

    @staticmethod
    def run_command(command, env={}):
        print("Running", command)
        try:
            return subprocess.check_output(command, env=env)
        except subprocess.CalledProcessError as ex:
            if ex.stdout:
                print(ex.stdout.decode("utf-8"))
            if ex.stderr:
                print(ex.stderr.decode("utf-8"))
            raise

    def run_command_with_dds_env(self, command, dds_config_filename):
        self.run_command(command, env={DDSCLIENT_CONFIG_ENV: dds_config_filename})


class StageDataCommand(BaseCommand):
    def __init__(self, workflow, names, paths):
        self.workflow = workflow
        self.names = names
        self.paths = paths

    def command_file_dict(self, input_files):
        items = [
            self.create_stage_data_config_item(StageDataTypes.URL,
                                               self.workflow.workflow_url,
                                               self.names.workflow_download_dest,
                                               self.names.unzip_workflow_url_to_path),
            self.create_stage_data_config_item(StageDataTypes.WRITE,
                                               self.workflow.job_order,
                                               self.names.job_order_path)
        ]
        for dds_file in input_files.dds_files:
            dest = '{}/{}'.format(self.paths.JOB_DATA, dds_file.destination_path)
            items.append(self.create_stage_data_config_item(StageDataTypes.DUKEDS, dds_file.file_id, dest))
        return {"items": items}

    @staticmethod
    def create_stage_data_config_item(workflow_type, source, dest, unzip_to=None):
        item = {"type": workflow_type, "source": source, "dest": dest}
        if unzip_to:
            item["unzip_to"] = unzip_to
        return item

    def run(self, base_command, dds_credentials, input_files):
        command_filename = self.names.stage_data_command_filename
        self.write_json_file(command_filename, self.command_file_dict(input_files))

        dds_config_filename = self.names.dds_config_filename
        self.write_dds_config_file(dds_config_filename, dds_credentials)

        command = base_command.copy()
        command.append(command_filename)
        command.append(self.names.workflow_input_files_metadata_path)
        return self.run_command_with_dds_env(command, dds_config_filename)


class OrganizeOutputCommand(BaseCommand):
    def __init__(self, job, names, paths):
        self.job = job
        self.names = names
        self.paths = paths

    def command_file_dict(self, methods_document_content):
        additional_log_files = []
        if self.names.usage_report_path:
            additional_log_files.append(self.names.usage_report_path)
        return {
            "bespin_job_id": self.job.id,
            "destination_dir": self.paths.OUTPUT_RESULTS_DIR,
            "downloaded_workflow_path": self.names.workflow_download_dest,
            "workflow_to_read": self.names.workflow_to_read,
            "workflow_type": self.job.workflow.workflow_type,
            "job_order_path": self.names.job_order_path,
            "bespin_workflow_stdout_path": self.names.run_workflow_stdout_path,
            "bespin_workflow_stderr_path": self.names.run_workflow_stderr_path,
            "methods_template": methods_document_content,
            "additional_log_files": additional_log_files
        }

    def run(self, base_command, methods_document_content):
        command_filename = self.names.organize_output_command_filename
        self.write_json_file(command_filename, self.command_file_dict(methods_document_content))
        command = base_command.copy()
        command.append(self.names.organize_output_command_filename)
        return self.run_command(command)


class SaveOutputCommand(BaseCommand):
    def __init__(self, names, paths, activity_name, activity_description):
        self.names = names
        self.paths = paths
        self.activity_name = activity_name
        self.activity_description = activity_description

    def command_file_dict(self, share_dds_ids):
        return {
            "destination": self.names.output_project_name,
            "readme_file_path": self.paths.REMOTE_README_FILE_PATH,
            "paths": [self.paths.OUTPUT_RESULTS_DIR],
            "share": {
                "dds_user_ids": share_dds_ids
            },
            "activity": {
                "name": self.activity_name,
                "description": self.activity_description,
                "started_on": "",
                "ended_on": "",
                "input_file_versions_json_path": self.names.workflow_input_files_metadata_path,
                "workflow_output_json_path": self.names.run_workflow_stdout_path
            }
        }

    def run(self, base_command, dds_credentials, share_dds_ids):
        command_filename = self.names.save_output_command_filename
        self.write_json_file(command_filename, self.command_file_dict(share_dds_ids))

        dds_config_filename = self.names.dds_config_filename
        self.write_dds_config_file(dds_config_filename, dds_credentials)

        command = base_command.copy()
        command.append(command_filename)
        command.append(self.names.output_project_details_filename)
        command.append("--outfile-format")
        command.append("json")
        return self.run_command_with_dds_env(command, dds_config_filename)

    def get_project_details(self):
        with open(self.names.output_project_details_filename) as infile:
            return json.load(infile)


class ZippedWorkflowNames(object):
    def __init__(self, job_workflow, workflow_base_dir, workflow_download_dest):
        self.workflow_download_dest = workflow_download_dest
        self.workflow_to_run = '{}/{}'.format(workflow_base_dir, job_workflow.workflow_path)
        self.workflow_to_read = self.workflow_to_run
        self.unzip_workflow_url_to_path = workflow_base_dir


class PackedWorkflowNames(object):
    def __init__(self, job_workflow, workflow_download_dest):
        self.workflow_download_dest = workflow_download_dest
        self.workflow_to_run = '{}{}'.format(self.workflow_download_dest, job_workflow.workflow_path)
        self.workflow_to_read = self.workflow_download_dest
        self.unzip_workflow_url_to_path = None


def create_workflow_names(job, paths):
    job_workflow = job.workflow
    workflow_type = job_workflow.workflow_type
    workflow_download_dest = '{}/{}'.format(paths.WORKFLOW, os.path.basename(job_workflow.workflow_url))
    if workflow_type == WorkflowTypes.ZIPPED:
        return ZippedWorkflowNames(job_workflow, paths.WORKFLOW, workflow_download_dest)
    elif workflow_type == WorkflowTypes.PACKED:
        return PackedWorkflowNames(job_workflow, workflow_download_dest)
    else:
        raise ValueError("Unknown workflow type {}".format(workflow_type))
