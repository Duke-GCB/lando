from lando.common.names import WorkflowNames, WorkflowTypes, BaseNames, Paths
from unittest import TestCase
from unittest.mock import Mock, patch


class TestWorkflowNames(TestCase):
    def setUp(self):
        self.zipped_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.zipped_job.workflow.workflow_type = WorkflowTypes.ZIPPED
        self.packed_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.packed_job.workflow.workflow_type = WorkflowTypes.PACKED
        self.invalid_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.invalid_job.workflow.workflow_type = 'other'
        self.mock_names = Mock(WORKFLOW='/workflowdir')

    @patch('lando.common.names.ZippedWorkflowNames')
    def test_zipped_type(self, mock_zipped_workflow_names):
        names = WorkflowNames(self.zipped_job, self.mock_names)
        self.assertEqual(names._names_target, mock_zipped_workflow_names.return_value)
        mock_zipped_workflow_names.assert_called_with(self.zipped_job.workflow, '/workflowdir', '/workflowdir/someurl')

    @patch('lando.common.names.PackedWorkflowNames')
    def test_packed_type(self, mock_packed_workflow_names):
        names = WorkflowNames(self.packed_job, self.mock_names)
        self.assertEqual(names._names_target, mock_packed_workflow_names.return_value)
        mock_packed_workflow_names.assert_called_with(self.packed_job.workflow, '/workflowdir/someurl')

    def test_invalid_type(self):
        with self.assertRaises(ValueError) as raised_exception:
            WorkflowNames(self.invalid_job, self.mock_names)
        self.assertEqual(str(raised_exception.exception), 'Unknown workflow type other')


class BaseNamesTestCase(TestCase):
    @patch('lando.common.names.dateutil')
    def test_packed_workflow(self, mock_dateutil):
        mock_dateutil.parser.parse.return_value.strftime.return_value = 'somedate'
        paths, job = Mock(), Mock()
        paths.JOB_DATA = '/job-data'
        paths.OUTPUT_DATA = '/output-data'
        paths.CONFIG_DIR = '/config'
        paths.WORKFLOW = '/workflowdir'
        job.id = 49
        job.name = 'myjob'
        job.username = 'joe'
        job.workflow.name = 'myworkflow'
        job.workflow.version = 2
        job.workflow.workflow_url = 'someurl/workflow.cwl'
        job.workflow.workflow_type = WorkflowTypes.PACKED
        job.workflow.workflow_path = ''
        names = BaseNames(job, paths)

        self.assertEqual(names.job_order_path, '/job-data/job-order.json')
        self.assertEqual(names.run_workflow_stdout_path, '/output-data/bespin-workflow-output.json')
        self.assertEqual(names.run_workflow_stderr_path, '/output-data/bespin-workflow-output.log')
        self.assertEqual(names.output_project_name, 'Bespin myworkflow v2 myjob somedate')
        self.assertEqual(names.workflow_input_files_metadata_path, '/job-data/workflow-input-files-metadata.json')
        self.assertEqual(names.usage_report_path, '/output-data/job-49-joe-resource-usage.json')
        self.assertEqual(names.activity_name, 'myjob - Bespin Job 49')
        self.assertEqual(names.activity_description, 'Bespin Job 49 - Workflow myworkflow v2')
        self.assertEqual(names.workflow_download_dest, '/workflowdir/workflow.cwl')
        self.assertEqual(names.workflow_to_run, '/workflowdir/workflow.cwl')
        self.assertEqual(names.workflow_to_read, '/workflowdir/workflow.cwl')
        self.assertEqual(names.unzip_workflow_url_to_path, None)

    @patch('lando.common.names.dateutil')
    def test_zipped_workflow(self, mock_dateutil):
        mock_dateutil.parser.parse.return_value.strftime.return_value = 'somedate'
        paths, job = Mock(), Mock()
        paths.JOB_DATA = '/job-data'
        paths.OUTPUT_DATA = '/output-data'
        paths.CONFIG_DIR = '/config'
        paths.WORKFLOW = '/workflowdir'
        job.id = 49
        job.name = 'myjob'
        job.username = 'joe'
        job.workflow.name = 'myworkflow'
        job.workflow.version = 2
        job.workflow.workflow_url = 'someurl'
        job.workflow.workflow_type = WorkflowTypes.ZIPPED
        job.workflow.workflow_path = 'workflow.cwl'
        names = BaseNames(job, paths)

        self.assertEqual(names.workflow_download_dest, '/workflowdir/someurl')
        self.assertEqual(names.workflow_to_run, '/workflowdir/workflow.cwl')
        self.assertEqual(names.workflow_to_read, '/workflowdir/workflow.cwl')
        self.assertEqual(names.unzip_workflow_url_to_path, '/workflowdir')


class PathsTestCase(TestCase):
    def test_root_base_dir(self):
        paths = Paths(base_directory='/')
        self.assertEqual(paths.OUTPUT_RESULTS_DIR, '/bespin/output-data/results')
        self.assertEqual(paths.JOB_DATA, '/bespin/job-data')

    def test_working_base_dir(self):
        paths = Paths(base_directory='/work/')
        self.assertEqual(paths.OUTPUT_RESULTS_DIR, '/work/bespin/output-data/results')
        self.assertEqual(paths.JOB_DATA, '/work/bespin/job-data')
