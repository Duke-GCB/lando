from __future__ import absolute_import
from unittest import TestCase
from lando.worker.provenance import WorkflowFiles, DukeDSProjectInfo, WorkflowActivity
from mock import patch, Mock, call, mock_open


class TestWorkflowFiles(TestCase):
    @patch('lando.worker.provenance.os.walk')
    def test_get_output_filenames(self, mock_walk):
        workflow_files = WorkflowFiles(working_directory='/tmp', job_id=1, workflow_filename="fastqc.cwl")
        mock_walk.return_value = [
            ('/tmp', '', ['data.txt', 'data2.txt'])
        ]
        expected_filenames = ['/tmp/data.txt', '/tmp/data2.txt']
        self.assertEqual(expected_filenames, workflow_files.get_output_filenames())
        mock_walk.assert_called_with('/tmp/results')

    def test_get_input_filenames(self):
        workflow_files = WorkflowFiles(working_directory='/tmp', job_id=1, workflow_filename="fastqc.cwl")
        expected_filenames = ['/tmp/results/documentation/scripts/fastqc.cwl',
                              '/tmp/results/documentation/scripts/job-1-input.yml']
        self.assertEqual(expected_filenames, workflow_files.get_input_filenames())

    def test_get_job_data(self):
        workflow_files = WorkflowFiles(working_directory='/tmp', job_id=1, workflow_filename="fastqc.cwl")
        read_data = '{"started": "2017-06-01:0800",  "finished": "2017-06-01:0830"}'
        with patch("__builtin__.open", mock_open(read_data=read_data)) as mock_file:
            job_data = workflow_files.get_job_data()
        expected_job_data = {
            "started": "2017-06-01:0800",
            "finished": "2017-06-01:0830",
        }
        self.assertEqual(expected_job_data, job_data)


class TestDukeDSProjectInfo(TestCase):
    def test_file_id_lookup(self):
        mock_file1 = Mock(kind='dds-file', remote_id='123', path='/tmp/data.txt')
        mock_file2 = Mock(kind='dds-file', remote_id='124', path='/tmp/data2.txt')
        mock_folder = Mock(kind='dds-folder', children=[mock_file2])
        mock_project = Mock(kind='dds-project', children=[mock_file1, mock_folder])
        project_info = DukeDSProjectInfo(project=mock_project)
        expected_dictionary = {'/tmp/data.txt': '123', '/tmp/data2.txt': '124'}
        self.assertEqual(expected_dictionary, project_info.file_id_lookup)


class TestWorkflowActivity(TestCase):
    def setUp(self):
        mock_file1 = Mock(kind='dds-file', remote_id='123', path='/tmp/results/documentation/scripts/example.cwl')
        mock_file2 = Mock(kind='dds-file', remote_id='124', path='/tmp/results/documentation/scripts/job-444-input.yml')
        mock_file3 = Mock(kind='dds-file', remote_id='125', path='/tmp/results/data.txt')
        mock_folder1 = Mock(name='scripts', kind='dds-folder', children=[mock_file1, mock_file2])
        mock_folder2 = Mock(name='output', kind='dds-folder', children=[mock_file3])
        self.mock_project = Mock(kind='dds-project', children=[mock_folder1, mock_folder2])

    @patch('lando.worker.provenance.WorkflowFiles')
    def test_getters(self, mock_workflow_files):
        workflow = Mock(version='1', url='http://something/example.cwl')
        workflow.name = 'RnaSeq'
        job_details = Mock(id='444', workflow=workflow)
        job_details.name = 'Myjob'
        workflow_activity = WorkflowActivity(
            job_details=job_details,
            working_directory='/tmp/',
            project=self.mock_project)
        self.assertEqual('Myjob - Bespin Job 444', workflow_activity.get_name())
        self.assertEqual('Bespin Job 444 - Workflow RnaSeq v1', workflow_activity.get_description())
        mock_workflow_files.return_value.get_job_data.return_value = {
            "started": "12:00",
            "finished": "12:30",
        }
        self.assertEqual("12:00", workflow_activity.get_started_on())
        self.assertEqual("12:30", workflow_activity.get_ended_on())

    def test_used_file_ids_returns_job_input_and_cwl(self):
        workflow_activity = WorkflowActivity(
            job_details=Mock(id='444', workflow=Mock(name='RnaSeq', version='1', url='http://something/example.cwl')),
            working_directory='/tmp/',
            project=self.mock_project)
        file_ids = workflow_activity.get_used_file_ids()
        self.assertEqual(['123', '124'], file_ids)

    @patch('lando.worker.provenance.WorkflowFiles')
    def test_generated_file_ids_returns_output_files(self, mock_workflow_files):
        mock_workflow_files.return_value.get_output_filenames.return_value = ['/tmp/results/data.txt']
        workflow_activity = WorkflowActivity(
            job_details=Mock(id='444', workflow=Mock(name='RnaSeq', version='1', url='http://something/example.cwl')),
            working_directory='/tmp/',
            project=self.mock_project)
        file_ids = workflow_activity.get_generated_file_ids()
        self.assertEqual(['125'], file_ids)
