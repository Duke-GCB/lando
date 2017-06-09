from __future__ import absolute_import
from unittest import TestCase
from lando.worker.provenance import WorkflowFiles, DukeDSProjectInfo, WorkflowActivityFiles
from mock import patch, Mock, call


class TestWorkflowFiles(TestCase):
    @patch('lando.worker.provenance.os.walk')
    def test_get_output_filenames(self, mock_walk):
        workflow_files = WorkflowFiles(working_directory='/tmp', job_id=1, workflow_filename="fastqc.cwl")
        mock_walk.return_value = [
            ('/tmp', '', ['data.txt', 'data2.txt'])
        ]
        expected_filenames = ['/tmp/data.txt', '/tmp/data2.txt']
        self.assertEqual(expected_filenames, workflow_files.get_output_filenames())
        mock_walk.assert_called_with('/tmp/output')

    def test_get_input_filenames(self):
        workflow_files = WorkflowFiles(working_directory='/tmp', job_id=1, workflow_filename="fastqc.cwl")
        expected_filenames = ['/tmp/scripts/fastqc.cwl', '/tmp/scripts/job-1-input.yml']
        self.assertEqual(expected_filenames, workflow_files.get_input_filenames())


class TestDukeDSProjectInfo(TestCase):
    def test_stuff(self):
        mock_file1 = Mock(kind='dds-file', remote_id='123', path='/tmp/data.txt')
        mock_file2 = Mock(kind='dds-file', remote_id='124', path='/tmp/data2.txt')
        mock_folder = Mock(kind='dds-folder', children=[mock_file2])
        mock_project = Mock(kind='dds-project', children=[mock_file1, mock_folder])
        project_info = DukeDSProjectInfo(project=mock_project)
        expected_dictionary = {'/tmp/data.txt': '123', '/tmp/data2.txt': '124'}
        self.assertEqual(expected_dictionary, project_info.file_id_lookup)


class TestWorkflowActivityFiles(TestCase):
    def setUp(self):
        mock_file1 = Mock(kind='dds-file', remote_id='123', path='/tmp/data.txt')
        mock_file2 = Mock(kind='dds-file', remote_id='124', path='/tmp/data2.txt')
        mock_folder = Mock(kind='dds-folder', children=[mock_file2])
        mock_project = Mock(kind='dds-project', children=[mock_file1, mock_folder])
        workflow_files = Mock()
        workflow_files.get_input_filenames.return_value = ['/tmp/data.txt']
        workflow_files.get_output_filenames.return_value = ['/tmp/data2.txt']
        self.workflow_activity_files = WorkflowActivityFiles(workflow_files, mock_project)

    def test_used_file_ids(self):
        file_ids = self.workflow_activity_files.get_used_file_ids()
        self.assertEqual(['123'], file_ids)

    def test_generated_file_ids(self):
        file_ids = self.workflow_activity_files.get_generated_file_ids()
        self.assertEqual(['124'], file_ids)
