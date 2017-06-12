from __future__ import absolute_import
from unittest import TestCase
from lando.worker.staging import SaveJobOutput
from mock import patch, Mock, MagicMock


class TestSaveJobOutput(TestCase):
    def setUp(self):
        payload = MagicMock()
        payload.job_details.workflow.name = 'SomeWorkflow'
        payload.job_details.workflow.version = 2
        payload.job_details.name = 'MyJob'
        payload.job_details.created = '2017-03-21T13:29:09.123603Z'
        payload.job_details.username = 'john@john.org'
        self.payload = payload

    def test_create_project_name(self):
        name = SaveJobOutput.create_project_name(self.payload)
        self.assertEqual("Bespin SomeWorkflow v2 MyJob 2017-29-21", name)

    def test_get_dukeds_username(self):
        self.payload.job_details.username = 'joe@joe.com'
        save_job_output=SaveJobOutput(self.payload)
        self.assertEqual('joe', save_job_output.get_dukeds_username())
        self.payload.job_details.username = 'bob123'
        save_job_output=SaveJobOutput(self.payload)
        self.assertEqual('bob123', save_job_output.get_dukeds_username())

    @patch('lando.worker.staging.Context')
    @patch('lando.worker.staging.os.listdir')
    @patch('lando.worker.provenance.WorkflowActivity')
    @patch('lando.worker.staging.ProjectUpload')
    def test_run(self, mock_project_upload, mock_activity, mock_listdir, mock_context):
        mock_listdir.return_value = ['output', 'scripts']
        save_job_output = SaveJobOutput(self.payload)
        save_job_output.run('/tmp/jobresults')
        mock_project_upload().run.assert_called()

        data_service = mock_context().get_duke_data_service()
        # We should create an activity
        data_service.create_activity.assert_called()
        data_service.create_used_relations.assert_called()
        data_service.create_generated_by_relations.assert_called()

        # We should give permissions to the user
        give_user_permissions = data_service.give_user_permissions
        give_user_permissions.assert_called_with(mock_project_upload().local_project.remote_id, 'john',
                                                 auth_role='project_admin')
