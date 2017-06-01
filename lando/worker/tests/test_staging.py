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

    @patch('lando.worker.staging.ProjectUpload')
    @patch('lando.worker.staging.RemoteStore')
    def test_run(self, mock_remote_store, mock_project_upload):
        mock_project_upload().local_project = Mock(remote_id='1234')
        mock_remote_store().lookup_user_by_username.return_value = Mock(id='4567')
        self.payload.job_details.username = 'joe123@something.org'

        save_job_output = SaveJobOutput(self.payload)
        save_job_output.run(['/tmp/jobresults'])

        # We should upload the resulting directory into a new project
        mock_project_upload().run.assert_called()

        # We should give permissions to the user
        lookup_user_func = mock_remote_store().lookup_user_by_username
        lookup_user_func.assert_called_with('joe123')
        set_project_permission_func = mock_remote_store().data_service.set_user_project_permission
        set_project_permission_func.assert_called_with('1234',
                                                       '4567', 'project_admin')
