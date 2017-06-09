from __future__ import absolute_import
import os
from lando.worker.cwlworkflow import OUTPUT_DIRECTORY, WORKFLOW_DIRECTORY
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


class WorkflowActivityFiles(object):
    def __init__(self, workflow_files, local_project):
        """
        :param workflow_files: WorkflowFiles: knows paths to files on disk
        :param local_project: ddsc.core.localproject.LocalProject: LocalProject that was uploaded to DukeDS knows file ids
        """
        self.workflow_files = workflow_files
        self.duke_ds_project_info = DukeDSProjectInfo(local_project)

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


