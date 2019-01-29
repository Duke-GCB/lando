from unittest import TestCase
from unittest.mock import Mock, call
from lando.k8s.jobmanager import JobManager, JobStepTypes, FieldRefEnvVar
import json


class TestJobManager(TestCase):
    def setUp(self):
        mock_job_order = {
            'threads': 2
        }
        self.mock_job = Mock(username='jpb', workflow=Mock(url='someurl', job_order=mock_job_order), volume_size=3)
        self.mock_job.id = '51'
        self.mock_job.vm_settings.cwl_commands.base_command = ['cwltool']
        self.mock_job.vm_settings.image_name = 'calrissian:latest'

    def test_make_job_labels(self):
        manager = JobManager(cluster_api=Mock(), job_settings=Mock(), job=self.mock_job)
        expected_label_dict = {
            'bespin-job': 'true',
            'bespin-job-id': '51',
            'bespin-job-step': 'stage_data'
        }
        self.assertEqual(manager.make_job_labels(job_step_type=JobStepTypes.STAGE_DATA), expected_label_dict)

    def test_create_stage_data_persistent_volumes(self):
        manager = JobManager(cluster_api=Mock(), job_settings=Mock(), job=self.mock_job)
        manager.create_stage_data_persistent_volumes()
        manager.cluster_api.create_persistent_volume_claim.assert_has_calls([
            call('job-data-51-jpb', storage_class_name=manager.storage_class_name, storage_size_in_g=3)
        ])

    def test_create_stage_data_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock()
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)
        mock_input_files = Mock(dds_files=[
            Mock(destination_path='file1.txt', file_id='myid')
        ])
        manager.create_stage_data_job(input_files=mock_input_files)

        # it should have created a config map of what needs to be staged
        config_map_payload = {
            'stagedata.json': json.dumps({
                "items": [
                    {
                        "type": "url",
                        "source": "someurl",
                        "dest": "/bespin/job-data/workflow/someurl"
                    },
                    {
                        "type": "write",
                        "source": {"threads": 2},
                        "dest": "/bespin/job-data/job-order.json"
                    },
                    {
                        "type": "DukeDS",
                        "source": "myid",
                        "dest": "/bespin/job-data/file1.txt"
                    }
                ]
            })
        }
        mock_cluster_api.create_config_map.assert_called_with(name='stage-data-51-jpb',
                                                              data=config_map_payload)

        # it should have created a job
        args, kwargs = mock_cluster_api.create_job.call_args
        name, batch_spec = args
        self.assertEqual(name, 'stage-data-51-jpb')  # job name
        self.assertEqual(batch_spec.name, 'stage-data-51-jpb')  # job spec name
        self.assertEqual(batch_spec.labels['bespin-job-id'], '51')  # Bespin job id stored in a label
        self.assertEqual(batch_spec.labels['bespin-job-step'], 'stage_data')  # store the job step in a label
        job_container = batch_spec.container
        self.assertEqual(job_container.name, 'stage-data-51-jpb')  # container name
        self.assertEqual(job_container.image_name, mock_config.stage_data_settings.image_name,
                         'stage data image name is based on a config setting')
        self.assertEqual(job_container.command, mock_config.stage_data_settings.command,
                         'stage data command is based on a config setting')
        self.assertEqual(job_container.args, ['/bespin/config/stagedata.json'],
                         'stage data command should receive config file as an argument')
        self.assertEqual(job_container.env_dict, {'DDSCLIENT_CONF': '/etc/ddsclient/config'},
                         'DukeDS environment variable should point to the config mapped config file')
        self.assertEqual(job_container.requested_cpu, mock_config.stage_data_settings.requested_cpu,
                         'stage data requested cpu is based on a config setting')
        self.assertEqual(job_container.requested_memory, mock_config.stage_data_settings.requested_memory,
                         'stage data requested memory is based on a config setting')
        self.assertEqual(len(job_container.volumes), 3)

        user_data_volume = job_container.volumes[0]
        self.assertEqual(user_data_volume.name, 'job-data-51-jpb')
        self.assertEqual(user_data_volume.mount_path, '/bespin/job-data')
        self.assertEqual(user_data_volume.volume_claim_name, 'job-data-51-jpb')
        self.assertEqual(user_data_volume.read_only, False)

        config_map_volume = job_container.volumes[1]
        self.assertEqual(config_map_volume.name, 'stage-data-51-jpb')
        self.assertEqual(config_map_volume.mount_path, '/bespin/config')
        self.assertEqual(config_map_volume.config_map_name, 'stage-data-51-jpb')
        self.assertEqual(config_map_volume.source_key, 'stagedata.json')
        self.assertEqual(config_map_volume.source_path, 'stagedata.json')

        secret_volume = job_container.volumes[2]
        self.assertEqual(secret_volume.name, 'data-store-51-jpb')
        self.assertEqual(secret_volume.mount_path, '/etc/ddsclient')
        self.assertEqual(secret_volume.secret_name, mock_config.data_store_settings.secret_name,
                         'name of DukeDS secret is based on a config setting')

    def test_cleanup_stage_data_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock()
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.cleanup_stage_data_job()

        mock_cluster_api.delete_job.assert_called_with('stage-data-51-jpb')
        mock_cluster_api.delete_config_map.assert_called_with('stage-data-51-jpb')

    def test_create_run_workflow_persistent_volumes(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.create_run_workflow_persistent_volumes()

        mock_cluster_api.create_persistent_volume_claim.assert_has_calls([
            call('tmpout-51-jpb', storage_class_name='nfs', storage_size_in_g=3),
            call('output-data-51-jpb', storage_class_name='nfs', storage_size_in_g=3),
            call('tmp-51-jpb', storage_class_name='nfs', storage_size_in_g=1),
        ])

    def test_create_run_workflow_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.create_run_workflow_job()

        # it should have created a job to run the workflow with several volumes mounted
        args, kwargs = mock_cluster_api.create_job.call_args
        name, batch_spec = args
        self.assertEqual(name, 'run-workflow-51-jpb')  # job name
        self.assertEqual(batch_spec.name, 'run-workflow-51-jpb')  # job spec name
        self.assertEqual(batch_spec.labels['bespin-job-id'], '51')  # Bespin job id stored in a label
        self.assertEqual(batch_spec.labels['bespin-job-step'], 'run_workflow')  # store the job step in a label
        job_container = batch_spec.container
        self.assertEqual(job_container.name, 'run-workflow-51-jpb')  # container name
        self.assertEqual(job_container.image_name, self.mock_job.vm_settings.image_name,
                         'run workflow image name is based on job settings')

        self.assertEqual(job_container.command, ['bash', '-c', 'cwltool --tmp-outdir-prefix /bespin/tmpout/ '
                                                               '--outdir /bespin/output-data/ '
                                                               '/bespin/job-data/workflow/someurl '
                                                               '/bespin/job-data/job-order.json'],
                         'run workflow command combines job settings and staged files')
        self.assertEqual(job_container.env_dict['CALRISSIAN_POD_NAME'].field_path, 'metadata.name',
                         'We should store the pod name in a CALRISSIAN_POD_NAME environment variable')
        self.assertEqual(job_container.requested_cpu, mock_config.run_workflow_settings.requested_cpu,
                         'run workflow requested cpu is based on a config setting')
        self.assertEqual(job_container.requested_memory, mock_config.run_workflow_settings.requested_memory,
                         'run workflow requested memory is based on a config setting')

        self.assertEqual(len(job_container.volumes), 5)

        tmp_volume = job_container.volumes[0]
        self.assertEqual(tmp_volume.name, 'tmp-51-jpb')
        self.assertEqual(tmp_volume.mount_path, '/tmp')
        self.assertEqual(tmp_volume.volume_claim_name, 'tmp-51-jpb')
        self.assertEqual(tmp_volume.read_only, False)

        job_data_volume = job_container.volumes[1]
        self.assertEqual(job_data_volume.name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.mount_path, '/bespin/job-data')
        self.assertEqual(job_data_volume.volume_claim_name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.read_only, True, 'job data should be a read only volume')

        output_data_volume = job_container.volumes[2]
        self.assertEqual(output_data_volume.name, 'output-data-51-jpb')
        self.assertEqual(output_data_volume.mount_path, '/bespin/output-data')
        self.assertEqual(output_data_volume.volume_claim_name, 'output-data-51-jpb')
        self.assertEqual(output_data_volume.read_only, False)

        tmpout_volume = job_container.volumes[3]
        self.assertEqual(tmpout_volume.name, 'tmpout-51-jpb')
        self.assertEqual(tmpout_volume.mount_path, '/bespin/tmpout')
        self.assertEqual(tmpout_volume.volume_claim_name, 'tmpout-51-jpb')
        self.assertEqual(tmpout_volume.read_only, False)

        system_data_volume = job_container.volumes[4]
        self.assertEqual(system_data_volume.name, 'system-data-51-jpb')
        self.assertEqual(system_data_volume.mount_path,
                         mock_config.run_workflow_settings.system_data_volume.mount_path,
                         'mount path for the system volume is based on a config setting')
        self.assertEqual(system_data_volume.volume_claim_name,
                         mock_config.run_workflow_settings.system_data_volume.volume_claim_name,
                         'pvc name for the system volume is based on a config setting')
        self.assertEqual(system_data_volume.read_only, True,
                         'system data should be read only for running workflow')

    def test_cleanup_run_workflow_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.cleanup_run_workflow_job()

        mock_cluster_api.delete_job.assert_called_with('run-workflow-51-jpb')
        mock_cluster_api.delete_persistent_volume_claim.assert_has_calls([
            call('tmpout-51-jpb'), call('tmp-51-jpb')
        ], 'delete tmp volumes once running workflow completes')

    def test_create_organize_output_project_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.create_organize_output_project_job()

        # it should have created a job to run the workflow with several volumes mounted
        args, kwargs = mock_cluster_api.create_job.call_args
        name, batch_spec = args
        self.assertEqual(name, 'organize-output-51-jpb')  # job name
        self.assertEqual(batch_spec.name, 'organize-output-51-jpb')  # job spec name
        self.assertEqual(batch_spec.labels['bespin-job-id'], '51')  # Bespin job id stored in a label
        self.assertEqual(batch_spec.labels['bespin-job-step'], 'organize_output')  # store the job step in a label
        job_container = batch_spec.container
        self.assertEqual(job_container.name, 'organize-output-51-jpb')  # container name
        self.assertEqual(job_container.image_name, mock_config.organize_output_settings.image_name,
                         'organize output image name is based on config settings')
        self.assertEqual(job_container.command, mock_config.organize_output_settings.command,
                         'organize output command is based on config settings')
        self.assertEqual(job_container.requested_cpu, mock_config.organize_output_settings.requested_cpu,
                         'organize output requested cpu is based on a config setting')
        self.assertEqual(job_container.requested_memory, mock_config.organize_output_settings.requested_memory,
                         'organize output requested memory is based on a config setting')

        self.assertEqual(len(job_container.volumes), 2)

        job_data_volume = job_container.volumes[0]
        self.assertEqual(job_data_volume.name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.mount_path, '/bespin/job-data')
        self.assertEqual(job_data_volume.volume_claim_name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.read_only, True, 'job data should be a read only volume')

        output_data_volume = job_container.volumes[1]
        self.assertEqual(output_data_volume.name, 'output-data-51-jpb')
        self.assertEqual(output_data_volume.mount_path, '/bespin/output-data')
        self.assertEqual(output_data_volume.volume_claim_name, 'output-data-51-jpb')
        self.assertEqual(output_data_volume.read_only, False)

    def test_cleanup_organize_output_project_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.cleanup_organize_output_project_job()

        mock_cluster_api.delete_job.assert_called_with('organize-output-51-jpb')

    def test_create_save_output_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.create_save_output_job()

        # it should have created a config map of what needs to be staged
        config_map_payload = {
            'saveoutput.json': json.dumps({
                "destination": "Bespin-job-51-results",
                "paths": ["/bespin/output-data"]
            })
        }
        mock_cluster_api.create_config_map.assert_called_with(name='save-output-51-jpb',
                                                              data=config_map_payload)

        # it should have created a job
        args, kwargs = mock_cluster_api.create_job.call_args
        name, batch_spec = args
        self.assertEqual(name, 'save-output-51-jpb')  # job name
        self.assertEqual(batch_spec.name, 'save-output-51-jpb')  # job spec name
        self.assertEqual(batch_spec.labels['bespin-job-id'], '51')  # Bespin job id stored in a label
        self.assertEqual(batch_spec.labels['bespin-job-step'], 'save_output')  # store the job step in a label
        job_container = batch_spec.container
        self.assertEqual(job_container.name, 'save-output-51-jpb')  # container name
        self.assertEqual(job_container.image_name, mock_config.save_output_settings.image_name,
                         'save output image name is based on a config setting')
        self.assertEqual(job_container.command, mock_config.save_output_settings.command,
                         'save output command is based on a config setting')
        self.assertEqual(job_container.args, ['/bespin/config/saveoutput.json'],
                         'save output command should receive config file as an argument')
        self.assertEqual(job_container.env_dict, {'DDSCLIENT_CONF': '/etc/ddsclient/config'},
                         'DukeDS environment variable should point to the config mapped config file')
        self.assertEqual(job_container.requested_cpu, mock_config.save_output_settings.requested_cpu,
                         'stage data requested cpu is based on a config setting')
        self.assertEqual(job_container.requested_memory, mock_config.save_output_settings.requested_memory,
                         'stage data requested memory is based on a config setting')
        self.assertEqual(len(job_container.volumes), 3)

        job_data_volume = job_container.volumes[0]
        self.assertEqual(job_data_volume.name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.mount_path, '/bespin/job-data')
        self.assertEqual(job_data_volume.volume_claim_name, 'job-data-51-jpb')
        self.assertEqual(job_data_volume.read_only, True)

        config_map_volume = job_container.volumes[1]
        self.assertEqual(config_map_volume.name, 'stage-data-51-jpb')
        self.assertEqual(config_map_volume.mount_path, '/bespin/config')
        self.assertEqual(config_map_volume.config_map_name, 'save-output-51-jpb')
        self.assertEqual(config_map_volume.source_key, 'saveoutput.json')
        self.assertEqual(config_map_volume.source_path, 'saveoutput.json')

        secret_volume = job_container.volumes[2]
        self.assertEqual(secret_volume.name, 'data-store-51-jpb')
        self.assertEqual(secret_volume.mount_path, '/etc/ddsclient')
        self.assertEqual(secret_volume.secret_name, mock_config.data_store_settings.secret_name,
                         'name of DukeDS secret is based on a config setting')

    def test_cleanup_save_output_job(self):
        mock_cluster_api = Mock()
        mock_config = Mock(storage_class_name='nfs')
        mock_job_settings = Mock(config=mock_config)
        manager = JobManager(cluster_api=mock_cluster_api, job_settings=mock_job_settings, job=self.mock_job)

        manager.cleanup_save_output_job()

        mock_cluster_api.delete_job.assert_called_with('save-output-51-jpb')
        mock_cluster_api.delete_config_map.assert_called_with('save-output-51-jpb')
        mock_cluster_api.delete_persistent_volume_claim.assert_has_calls([
            call('job-data-51-jpb'), call('output-data-51-jpb')
        ], 'delete job data and output data volumes once running workflow completes')