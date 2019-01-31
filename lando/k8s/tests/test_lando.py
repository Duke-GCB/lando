from lando.k8s.lando import K8sJobSettings, K8sJobActions, K8sLando, JobStates, JobSteps
import json
from unittest import TestCase
from unittest.mock import patch, Mock, call


class TestK8sJobSettings(TestCase):
    @patch('lando.k8s.lando.ClusterApi')
    def test_get_cluster_api(self, mock_cluster_api):
        mock_config = Mock()
        settings = K8sJobSettings(job_id=1, config=mock_config)
        cluster_api = settings.get_cluster_api()
        self.assertEqual(cluster_api, mock_cluster_api.return_value)
        mock_cluster_api.assert_called_with(
            mock_config.cluster_api_settings.host,
            mock_config.cluster_api_settings.token,
            mock_config.cluster_api_settings.namespace,
            verify_ssl=mock_config.cluster_api_settings.verify_ssl,
        )


class TestK8sJobActions(TestCase):
    def setUp(self):
        self.mock_settings = Mock(job_id='49')
        self.mock_job = Mock(state=JobStates.AUTHORIZED, step=JobSteps.NONE,
                             workflow=Mock(url='someurl.cwl'))
        self.mock_job.id = '49'
        self.actions = K8sJobActions(self.mock_settings)
        self.mock_job_api = self.mock_settings.get_job_api.return_value
        self.mock_job_api.get_job.return_value = self.mock_job

    @patch('lando.k8s.lando.ClusterApi')
    def test_constructor(self, mock_cluster_api):
        self.assertEqual(self.actions.cluster_api, self.mock_settings.get_cluster_api.return_value)

    @patch('lando.k8s.lando.JobManager')
    def test_make_job_manager(self, mock_job_manager):
        manager = self.actions.make_job_manager()
        self.assertEqual(manager, mock_job_manager.return_value)
        mock_job_manager.assert_called_with(
            self.mock_settings.get_cluster_api.return_value,
            self.mock_settings.config,
            self.mock_settings.get_job_api.return_value.get_job.return_value
        )

    def test_job_is_at_state_and_step(self):
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.STAGING

        self.assertTrue(self.actions.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.STAGING))
        self.assertFalse(self.actions.job_is_at_state_and_step(JobStates.RUNNING, JobSteps.RUNNING))
        self.assertFalse(self.actions.job_is_at_state_and_step(JobStates.ERRORED, JobSteps.STAGING))

    @patch('lando.k8s.lando.JobManager')
    def test_start_job(self, mock_job_manager):
        mock_input_file = Mock()
        self.mock_job_api.get_input_files.return_value = mock_input_file
        mock_manager = mock_job_manager.return_value
        k8s_job = Mock()
        k8s_job.metadata.name = 'job1'
        mock_manager.create_stage_data_job.return_value = k8s_job
        self.actions._show_status = Mock()

        self.actions.start_job(None)

        self.mock_job_api.set_job_state.assert_called_with(JobStates.RUNNING)
        self.mock_job_api.set_job_step.assert_has_calls([
            call(JobSteps.CREATE_VM),
            call(JobSteps.STAGING),
        ])
        mock_manager.create_stage_data_persistent_volumes.assert_called_with()
        mock_manager.create_stage_data_job.assert_called_with(mock_input_file)
        self.actions._show_status.assert_has_calls([
            call('Creating stage data persistent volumes'),
            call('Creating Stage data job'),
            call('Launched stage data job: job1'),
        ])

    @patch('lando.k8s.lando.JobManager')
    @patch('lando.k8s.lando.logging')
    def test_stage_job_complete_ignores_wrong_state_step(self, mock_logging, mock_job_manager):
        self.mock_job.state = JobStates.AUTHORIZED
        self.mock_job.step = JobSteps.NONE

        self.actions.stage_job_complete(None)

        mock_logging.info.assert_called_with("Ignoring request to run job:49 wrong step/state")
        self.mock_job_api._set_job_step.assert_not_called()

    @patch('lando.k8s.lando.JobManager')
    def test_stage_job_complete_with_valid_state_and_step(self, mock_job_manager):
        mock_manager = mock_job_manager.return_value
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.STAGING
        self.actions._show_status = Mock()
        k8s_job = Mock()
        k8s_job.metadata.name = 'job-45-john'
        mock_manager.create_run_workflow_job.return_value = k8s_job

        self.actions.stage_job_complete(None)

        self.mock_job_api.set_job_step.assert_called_with(JobSteps.RUNNING)
        mock_manager.cleanup_stage_data_job.assert_called_with()
        mock_manager.create_run_workflow_persistent_volumes.assert_called_with()
        mock_manager.create_run_workflow_job.assert_called_with()

        self.actions._show_status.assert_has_calls([
            call('Cleaning up after stage data'),
            call('Creating volumes for running workflow'),
            call('Creating run workflow job'),
            call('Launched run workflow job: job-45-john')
        ])

    @patch('lando.k8s.lando.JobManager')
    @patch('lando.k8s.lando.logging')
    def test_run_job_complete_ignores_wrong_state_step(self, mock_logging, mock_job_manager):
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.STORING_JOB_OUTPUT

        self.actions.run_job_complete(None)

        mock_logging.info.assert_called_with("Ignoring request to store output for job:49 wrong step/state")
        self.mock_job_api._set_job_step.assert_not_called()

    @patch('lando.k8s.lando.JobManager')
    def test_run_job_complete_valid_state_step(self, mock_job_manager):
        mock_manager = mock_job_manager.return_value
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.RUNNING
        self.actions._show_status = Mock()
        k8s_job = Mock()
        k8s_job.metadata.name = 'job-45-john'
        mock_manager.create_organize_output_project_job.return_value = k8s_job

        self.actions.run_job_complete(None)

        mock_manager.cleanup_run_workflow_job.assert_called_with()
        self.mock_job_api.set_job_step.assert_called_with(JobSteps.ORGANIZE_OUTPUT_PROJECT)
        mock_manager.create_organize_output_project_job.assert_called_with(
            self.mock_job_api.get_workflow_methods_document.return_value.content
        )
        self.mock_job_api.get_workflow_methods_document.assert_called_with(
            self.mock_job_api.get_job.return_value.workflow.methods_document
        )
        self.actions._show_status.assert_has_calls([
            call('Creating organize output project job'),
            call('Launched organize output project job: job-45-john'),
        ])

    @patch('lando.k8s.lando.JobManager')
    @patch('lando.k8s.lando.logging')
    def test_organize_output_complete_ignores_wrong_state_step(self, mock_logging, mock_job_manager):
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.STORING_JOB_OUTPUT

        self.actions.organize_output_complete(None)

        mock_logging.info.assert_called_with("Ignoring request to organize output project for job:49 wrong step/state")
        self.mock_job_api._set_job_step.assert_not_called()

    @patch('lando.k8s.lando.JobManager')
    def test_organize_output_complete_valid_state_step(self, mock_job_manager):
        mock_manager = mock_job_manager.return_value
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.ORGANIZE_OUTPUT_PROJECT
        self.actions._show_status = Mock()
        k8s_job = Mock()
        k8s_job.metadata.name = 'job-45-john'
        mock_manager.create_save_output_job.return_value = k8s_job

        self.actions.organize_output_complete(None)

        mock_manager.cleanup_organize_output_project_job.assert_called_with()
        mock_manager.create_save_output_job.assert_called_with(
            self.mock_job_api.get_store_output_job_data.return_value.share_dds_ids
        )
        self.actions._show_status.assert_has_calls([
            call('Creating store output job'),
            call('Launched save output job: job-45-john'),
        ])

    @patch('lando.k8s.lando.JobManager')
    @patch('lando.k8s.lando.logging')
    def test_store_job_output_complete_ignores_wrong_state_step(self, mock_logging, mock_job_manager):
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.NONE

        self.actions.store_job_output_complete(None)

        mock_logging.info.assert_called_with("Ignoring request to cleanup for job:49 wrong step/state")
        self.mock_job_api._set_job_step.assert_not_called()

    @patch('lando.k8s.lando.JobManager')
    def test_store_job_output_complete_valid_state_step(self, mock_job_manager):
        mock_manager = mock_job_manager.return_value
        self.mock_job.state = JobStates.RUNNING
        self.mock_job.step = JobSteps.STORING_JOB_OUTPUT
        self.actions._show_status = Mock()
        k8s_job = Mock()
        k8s_job.metadata.name = 'job-45-john'
        mock_manager.create_save_output_job.return_value = k8s_job
        mock_manager.read_save_output_pod_logs.return_value = json.dumps({"project_id": "1", "readme_file_id": "2"})

        self.actions.store_job_output_complete(None)

        mock_manager.cleanup_save_output_job.assert_called_with()
        self.actions._show_status.assert_has_calls([
            call('Marking job finished'),
        ])
        self.mock_job_api.save_project_details.assert_called_with('1', '2')

    @patch('lando.k8s.lando.JobManager')
    def test_cancel_job(self, mock_job_manager):
        mock_manager = mock_job_manager.return_value

        self.actions.cancel_job(None)

        self.mock_job_api.set_job_step.assert_called_with(JobSteps.NONE)
        self.mock_job_api.set_job_state.assert_called_with(JobStates.CANCELED)
        mock_manager.cleanup_all.assert_called_with()


class TestK8sLando(TestCase):
    @patch('lando.k8s.lando.ClusterApi')
    def test_constructor_creates_appropriate_job_actions(self, mock_cluster_api):
        mock_config = Mock()
        lando = K8sLando(mock_config)
        job_actions = lando._make_actions(job_id=2)
        self.assertEqual(job_actions.__class__.__name__, 'K8sJobActions')