from __future__ import absolute_import
import os
import json
from lando.worker.cwlworkflow import OUTPUT_DIRECTORY, WORKFLOW_DIRECTORY, LOGS_DIRECTORY, JOB_DATA_FILENAME
from ddsc.core.util import KindType


class WorkflowFiles(object):
    def __init__(self, working_directory, job_id, workflow_filename):
        """
        :param working_directory: str: directory containing output folders/files from running a workflow
        :param job_id: int: unique id for the job
        :param workflow_filename: str: name of the workflow file
        """
        self.working_directory = working_directory
        self.job_id = job_id
        self.workflow_filename = workflow_filename

    def get_output_filenames(self):
        """
        Get absolute paths for all files in the output directory.
        :return: [str]: list of file paths
        """
        output_dirname = os.path.join(self.working_directory, OUTPUT_DIRECTORY)
        output_filenames = []
        for root, dirnames, filenames in os.walk(output_dirname):
            for filename in filenames:
                full_filename = self._format_filename(os.path.join(root, filename))
                output_filenames.append(full_filename)
        return output_filenames

    def get_input_filenames(self):
        """
        Get absolute paths for the workflow and job input files.
        :return: [str]: list of file paths
        """
        scripts_dirname = os.path.join(self.working_directory, WORKFLOW_DIRECTORY)
        workflow_path = os.path.join(scripts_dirname, self.workflow_filename)
        job_input_path = os.path.join(scripts_dirname, 'job-{}-input.yml'.format(self.job_id))
        return [
            self._format_filename(workflow_path),
            self._format_filename(job_input_path)
        ]

    def get_job_data(self):
        job_data_path = os.path.join(self.working_directory, LOGS_DIRECTORY, JOB_DATA_FILENAME)
        with open(job_data_path, 'r') as infile:
            return json.load(infile)

    @staticmethod
    def _format_filename(filename):
        return os.path.abspath(filename)


class DukeDSProjectInfo(object):
    def __init__(self, project):
        """
        Contains file_id_lookup that goes from an absolute path -> file_id for files in project
        :param project: ddsc.core.localproject.LocalProject: LocalProject that was uploaded to DukeDS
        """
        self.file_id_lookup = self._build_file_id_lookup(project)

    @staticmethod
    def _build_file_id_lookup(project):
        """
        Creates dictionary from an absolute path to a file_id
        :param project: ddsc.core.localproject.LocalProject: LocalProject that was uploaded to DukeDS
        :return: dict: local_file_path -> duke_ds_file_id
        """
        lookup = {}
        for local_file in DukeDSProjectInfo._gather_files(project):
            lookup[local_file.path] = local_file.remote_id
        return lookup

    @staticmethod
    def _gather_files(project_node):
        """
        Fetch all files within project_node.
        :param project_node: container or file, if container returns children
        :return: [LocalFile]: list of files
        """
        if KindType.is_file(project_node):
            return [project_node]
        else:
            children_files = []
            for child in project_node.children:
                children_files.extend(DukeDSProjectInfo._gather_files(child))
            return children_files


class WorkflowActivity(object):
    def __init__(self, job_details, working_directory, project):
        """
        :param job_details: object: details about job(id, name, created date, workflow version)
        :param working_directory: str: directory that contains our output files
        :param project: ddsc.core.localproject.LocalProject: contains ids of uploaded files
        """
        self.job_details = job_details
        workflow_filename = os.path.basename(job_details.workflow.url)
        self.workflow_files = WorkflowFiles(working_directory, job_details.id, workflow_filename)
        self.duke_ds_project_info = DukeDSProjectInfo(project)

    def get_name(self):
        return "{} - Bespin Job {}".format(self.job_details.name, self.job_details.id)

    def get_description(self):
        return "Bespin Job {} - Workflow {} v{}".format(
            self.job_details.id,
            self.job_details.workflow.name,
            self.job_details.workflow.version)

    def get_started_on(self):
        return self.workflow_files.get_job_data()['started']

    def get_ended_on(self):
        return self.workflow_files.get_job_data()['finished']

    def get_used_file_ids(self):
        """
        Return the list off all workflow input file ids
        :return: [str]: list of DukeDS file uuids
        """
        file_ids = []
        for input_filename in self.workflow_files.get_input_filenames():
            file_id = self.duke_ds_project_info.file_id_lookup[input_filename]
            file_ids.append(file_id)
        return file_ids

    def get_generated_file_ids(self):
        """
        Return the list off all workflow output file ids
        :return: [str]: list of DukeDS file uuids
        """
        file_ids = []
        for output_filename in self.workflow_files.get_output_filenames():
            file_id = self.duke_ds_project_info.file_id_lookup[output_filename]
            file_ids.append(file_id)
        return file_ids


def create_activity(data_service, job_details, working_directory, project):
    """
    Creates a DukeDS provenance activity for our workflow. Creates used and generated by relations for
    files used/created by the workflow.
    :param data_service: staging.DukeDataService: used to create activity in DukeDS
    :param job_details: object: details about job(id, name, created date, workflow version)
    :param working_directory: str: directory that contains our output files
    :param project: ddsc.core.localproject.LocalProject: contains ids of uploaded files
    """
    workflow_activity = WorkflowActivity(job_details, working_directory, project)
    activity_id = data_service.create_activity(
        activity_name=workflow_activity.get_name(),
        desc=workflow_activity.get_description(),
        started_on=workflow_activity.get_started_on(),
        ended_on=workflow_activity.get_ended_on())
    data_service.create_used_relations(activity_id, workflow_activity.get_used_file_ids())
    data_service.create_generated_by_relations(activity_id, workflow_activity.get_generated_file_ids())
