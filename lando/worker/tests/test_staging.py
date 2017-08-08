from __future__ import absolute_import
from unittest import TestCase
from lando.worker.staging import SaveJobOutput, DukeDataService
from mock import patch, Mock, MagicMock, call


class TestSaveJobOutput(TestCase):
    def setUp(self):
        payload = MagicMock()
        payload.job_details.workflow.name = 'SomeWorkflow'
        payload.job_details.workflow.version = 2
        payload.job_details.name = 'MyJob'
        payload.job_details.created = '2017-03-21T13:29:09.123603Z'
        payload.job_details.username = 'john@john.org'
        payload.job_details.share_dds_ids = ['123','456']
        self.payload = payload

    def test_create_project_name(self):
        name = SaveJobOutput.create_project_name(self.payload)
        self.assertEqual("Bespin SomeWorkflow v2 MyJob 2017-03-21", name)

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
        share_project = data_service.share_project
        share_project.assert_has_calls([
            call('Bespin SomeWorkflow v2 MyJob 2017-03-21', '123'),
            call('Bespin SomeWorkflow v2 MyJob 2017-03-21', '456')
        ])


class TestDukeDataService(TestCase):
    @patch('lando.worker.staging.D4S2Project')
    @patch('lando.worker.staging.RemoteStore')
    def test_share_project(self, mock_remote_store, mock_d4s2_project):
        remote_user = Mock(id='132')
        mock_remote_store.return_value.fetch_user.return_value = remote_user
        data_service = DukeDataService(MagicMock())
        data_service.share_project('my_project', remote_user.id)
        mock_d4s2_project.return_value.share.assert_called_with('my_project', remote_user,
                                                                auth_role='project_admin', force_send=False,
                                                                user_message='Bespin job results.')
