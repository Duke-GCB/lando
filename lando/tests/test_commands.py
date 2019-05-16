from lando.commands import WorkflowNames, WorkflowTypes
from unittest import TestCase
from unittest.mock import Mock, patch


class TestWorkflowNamess(TestCase):
    def setUp(self):
        self.zipped_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.zipped_job.workflow.workflow_type = WorkflowTypes.ZIPPED
        self.packed_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.packed_job.workflow.workflow_type = WorkflowTypes.PACKED
        self.invalid_job = Mock(workflow=Mock(workflow_url="someurl"))
        self.invalid_job.workflow.workflow_type = 'other'
        self.mock_names = Mock(WORKFLOW='/workflowdir')

    @patch('lando.commands.ZippedWorkflowNames')
    def test_zipped_type(self, mock_zipped_workflow_names):
        names = WorkflowNames(self.zipped_job, self.mock_names)
        self.assertEqual(names._names_target, mock_zipped_workflow_names.return_value)
        mock_zipped_workflow_names.assert_called_with(self.zipped_job.workflow, '/workflowdir', '/workflowdir/someurl')

    @patch('lando.commands.PackedWorkflowNames')
    def test_packed_type(self, mock_packed_workflow_names):
        names = WorkflowNames(self.packed_job, self.mock_names)
        self.assertEqual(names._names_target, mock_packed_workflow_names.return_value)
        mock_packed_workflow_names.assert_called_with(self.packed_job.workflow, '/workflowdir/someurl')

    def test_invalid_type(self):
        with self.assertRaises(ValueError) as raised_exception:
            WorkflowNames(self.invalid_job, self.mock_names)
        self.assertEqual(str(raised_exception.exception), 'Unknown workflow type other')
