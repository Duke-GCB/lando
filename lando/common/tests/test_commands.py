from unittest import TestCase
from lando.common.commands import read_file, StepProcess, JobStepFailed, BaseCommand, StageDataCommand, \
    RunWorkflowCommand, OrganizeOutputCommand, SaveOutputCommand
from unittest.mock import patch, mock_open, call, ANY, Mock
from dateutil.parser import parse


class TestReadFile(TestCase):
    @patch('lando.common.commands.codecs')
    def test_reads_file_using_codecs(self, mock_codecs):
        expected_contents = 'Contents'
        mock_codecs.open.return_value.__enter__.return_value.read.return_value = expected_contents
        contents = read_file('myfile.txt')
        self.assertEqual(expected_contents, contents)
        mock_codecs.open.assert_called_with('myfile.txt','r',encoding='utf-8',errors='xmlcharrefreplace')

    @patch('lando.common.commands.codecs')
    def test_returns_empty_string_on_error(self, mock_codecs):
        mock_codecs.open.side_effect = OSError()
        contents = read_file('myfile.txt')
        self.assertEqual('', contents)


class StepProcessTestCase(TestCase):
    def test_total_runtime_str(self):
        step_process = StepProcess(command=['ls', '-l'], stdout_path='/tmp/stdout.txt', stderr_path='/tmp/stderr.txt')
        step_process.started = parse("2012-01-19 17:21:00")
        step_process.finished = parse("2012-01-19 17:24:00")
        self.assertEqual(step_process.total_runtime_str(), '3.0 minutes')

    @patch('lando.common.commands.logging')
    @patch('lando.common.commands.subprocess')
    @patch('lando.common.commands.datetime')
    def test_run_success(self, mock_datetime, mock_subprocess, mock_logging):
        mock_datetime.datetime.now.side_effect = [
            parse("2012-01-19 17:21:00"),
            parse("2012-01-19 17:24:00")
        ]
        mock_subprocess.call.return_value = 100
        step_process = StepProcess(command=['ls', '-l'], env={"MYKEY": "SECRET"},
                                   stdout_path='/tmp/stdout.txt',
                                   stderr_path='/tmp/stderr.txt')
        with patch("builtins.open", mock_open()) as fake_open:
            step_process.run()

        mock_subprocess.call.assert_called_with(['ls', '-l'], env={"MYKEY": "SECRET"},
                                                stderr=fake_open.return_value, stdout=fake_open.return_value)
        fake_open.assert_has_calls([
            call('/tmp/stdout.txt', 'w'), call('/tmp/stderr.txt', 'w')
        ])
        self.assertEqual(step_process.return_code, 100)
        self.assertEqual(step_process.started, parse("2012-01-19 17:21:00"))
        self.assertEqual(step_process.finished, parse("2012-01-19 17:24:00"))
        mock_logging.info.assert_has_calls([
            call('Running command: ls -l'),
            call('Redirecting stdout > /tmp/stdout.txt,  stderr > /tmp/stderr.txt')
        ])
        mock_logging.error.assert_not_called()

    @patch('lando.common.commands.logging')
    @patch('lando.common.commands.subprocess')
    def test_run_failure(self, mock_subprocess, mock_logging):
        mock_subprocess.call.side_effect = OSError()
        step_process = StepProcess(command=['ls', '-l'], stdout_path='/tmp/stdout.txt', stderr_path='/tmp/stderr.txt')
        with self.assertRaises(JobStepFailed) as raised_exception:
            with patch("builtins.open", mock_open()) as fake_open:
                step_process.run()
        self.assertEqual(raised_exception.exception.value, 'Command failed: ls -l')

        mock_subprocess.call.assert_called_with(['ls', '-l'], env=None,
                                                stdout=fake_open.return_value,
                                                stderr=fake_open.return_value)
        mock_logging.info.assert_has_calls([
            call('Running command: ls -l'),
            call('Redirecting stdout > /tmp/stdout.txt,  stderr > /tmp/stderr.txt')
        ])
        mock_logging.error.assert_has_calls([
            call('Error running subprocess %s', ANY)
        ])

    @patch('lando.common.commands.logging')
    @patch('lando.common.commands.subprocess')
    def test_run_with_stdout_and_stderr(self, mock_subprocess, mock_logging):
        mock_subprocess.call.return_value = 100
        fake_open = mock_open()
        with patch("builtins.open", fake_open):
            step_process = StepProcess(command=['ls', '-l'],
                                       stderr_path='/tmp/stderr.out',
                                       stdout_path='/tmp/stdout.log')
            step_process.run()

        mock_subprocess.call.assert_called_with(['ls', '-l'],
                                                env=None,
                                                stderr=fake_open.return_value,
                                                stdout=fake_open.return_value)
        self.assertEqual(step_process.return_code, 100)
        self.assertEqual(fake_open.call_count, 2)
        self.assertEqual(fake_open.return_value.close.call_count, 2)
        mock_logging.info.assert_has_calls([
            call('Running command: ls -l'),
            call('Redirecting stdout > /tmp/stdout.log,  stderr > /tmp/stderr.out')
        ])
        mock_logging.error.assert_not_called()


class BaseCommandTestCase(TestCase):
    @patch('lando.common.commands.json')
    def test_write_json_file(self, mock_json):
        cmd = BaseCommand()
        fake_open = mock_open()
        with patch("builtins.open", fake_open):
            cmd.write_json_file(filename='/tmp/data.json', data={"A": "B"})
        fake_open.assert_called_with("/tmp/data.json", 'w')
        fake_open.return_value.write.assert_called_with(mock_json.dumps.return_value)
        mock_json.dumps.assert_called_with({"A": "B"})

    @patch('lando.common.commands.StepProcess')
    @patch('lando.common.commands.tempfile')
    @patch('lando.common.commands.os')
    def test_run_command(self, mock_os, mock_tempfile, mock_step_process):
        file1 = Mock()
        file1.name = '/tmp/tempfile1.txt'
        file2 = Mock()
        file2.name = '/tmp/tempfile2.txt'
        mock_tempfile.NamedTemporaryFile.return_value.__enter__.side_effect = [file1, file2]
        mock_step_process.return_value.return_code = 0
        cmd = BaseCommand()
        result = cmd.run_command(command=['ls', '-l'])
        self.assertEqual(result, mock_step_process.return_value)
        mock_step_process.assert_called_with(['ls', '-l'], env=None,
                                             stdout_path='/tmp/tempfile1.txt',
                                             stderr_path='/tmp/tempfile2.txt')
        mock_step_process.return_value.run.assert_called()
        mock_os.remove.assert_has_calls([
            call('/tmp/tempfile1.txt'), call('/tmp/tempfile2.txt')
        ])

    @patch('lando.common.commands.StepProcess')
    @patch('lando.common.commands.tempfile')
    @patch('lando.common.commands.os')
    def test_run_command_with_stdout_stderr_filenames(self, mock_os, mock_tempfile, mock_step_process):
        mock_step_process.return_value.return_code = 0
        cmd = BaseCommand()
        result = cmd.run_command(command=['ls', '-l'], stderr_path='/tmp/stderr.txt', stdout_path='/tmp/stdout.txt')
        self.assertEqual(result, mock_step_process.return_value)
        mock_step_process.assert_called_with(['ls', '-l'], env=None,
                                             stdout_path='/tmp/stdout.txt',
                                             stderr_path='/tmp/stderr.txt')
        mock_os.remove.assert_not_called()
        mock_tempfile.NamedTemporaryFile.assert_not_called()
        mock_step_process.return_value.run.assert_called()

    @patch('lando.common.commands.StepProcess')
    @patch('lando.common.commands.read_file')
    @patch('lando.common.commands.tempfile')
    @patch('lando.common.commands.os')
    def test_run_command_bad_exit(self, mock_os, mock_tempfile, mock_read_file, mock_step_process):
        mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value.name = '/tmp/tempfile.txt'
        mock_read_file.side_effect = [
            'StdErr Msg', 'StdOut Msg'
        ]
        mock_step_process.return_value.return_code = 1
        cmd = BaseCommand()
        with self.assertRaises(JobStepFailed) as raised_exception:
            cmd.run_command(command=['ls', '-l'])
        self.assertEqual(raised_exception.exception.value, 'Process failed with exit code: 1\nStdErr Msg')
        self.assertEqual(raised_exception.exception.details, 'StdOut Msg')
        mock_step_process.assert_called_with(['ls', '-l'], env=None,
                                             stdout_path='/tmp/tempfile.txt',
                                             stderr_path='/tmp/tempfile.txt')
        mock_step_process.return_value.run.assert_called()

    @patch('lando.common.commands.os')
    def test_run_command_with_dds_env(self, mock_os):
        mock_os.environ.copy.return_value = {}
        cmd = BaseCommand()
        cmd.run_command = Mock()
        cmd.run_command_with_dds_env(['ddsclient', 'upload'], dds_config_filename='/tmp/ddsclient.conf')
        cmd.run_command.assert_called_with(
            ['ddsclient', 'upload'], env={'DDSCLIENT_CONF': '/tmp/ddsclient.conf'}
        )

    def test_dds_config_dict(self):
        credentials = Mock()
        credentials.endpoint_api_root = 'someurl'
        credentials.endpoint_agent_key = 'key'
        credentials.token = 'token'
        dds_dict = BaseCommand.dds_config_dict(credentials)
        self.assertEqual(dds_dict, {
            'agent_key': 'key',
            'url': 'someurl',
            'user_key': 'token'
        })

    def test_write_dds_config_file(self):
        credentials = Mock()
        credentials.endpoint_api_root = 'someurl'
        credentials.endpoint_agent_key = 'key'
        credentials.token = 'token'
        cmd = BaseCommand()
        cmd.write_json_file = Mock()
        cmd.write_dds_config_file('/tmp/ddsclient.conf', credentials)
        cmd.write_json_file.assert_called_with('/tmp/ddsclient.conf', {
            'agent_key': 'key',
            'url': 'someurl',
            'user_key': 'token'
        })


class StageDataCommandTestCase(TestCase):
    def setUp(self):
        self.mock_workflow = Mock(workflow_url='someurl', job_order={'a': 'b'})
        self.mock_names = Mock(workflow_download_dest='/tmp/results', unzip_workflow_url_to_path='/work',
                               job_order_path='/tmp/job-order.json', stage_data_command_filename='/work/cmd.json',
                               dds_config_filename='/work/ddsclient.conf',
                               workflow_input_files_metadata_path='/work/metadata.json')
        self.mock_paths = Mock(JOB_DATA='/work/job-data')
        self.input_files = Mock(dds_files=[
            Mock(file_id='123', destination_path='data.txt')
        ])

    def test_command_file_dict(self):
        cmd = StageDataCommand(self.mock_workflow, self.mock_names, self.mock_paths)
        self.assertEqual(cmd.command_file_dict(self.input_files), {
            'items': [
                {'dest': '/tmp/results', 'source': 'someurl', 'type': 'url', 'unzip_to': '/work'},
                {'dest': '/tmp/job-order.json', 'source': {'a': 'b'}, 'type': 'write'},
                {'dest': '/work/job-data/data.txt', 'source': '123', 'type': 'DukeDS'}
            ]
        })

    def test_run(self):
        cmd = StageDataCommand(self.mock_workflow, self.mock_names, self.mock_paths)
        cmd.write_json_file = Mock()
        cmd.run_command_with_dds_env = Mock()
        cmd.run(base_command=['downloadit'], dds_credentials=Mock(), input_files=self.input_files)
        cmd.write_json_file.assert_has_calls([
            call("/work/cmd.json", ANY),
            call("/work/ddsclient.conf", ANY),
        ])
        cmd.run_command_with_dds_env.assert_called_with(['downloadit', '/work/cmd.json', '/work/metadata.json'],
                                                        '/work/ddsclient.conf')


class RunWorkflowCommandTestCase(TestCase):
    def setUp(self):
        self.mock_job = Mock()
        self.mock_names = Mock(workflow_to_run='/work/workflow.cwl', job_order_path='/work/job-order.json',
                               usage_report_path='/work/usage.json', run_workflow_stderr_path='/work/stderr.log',
                               run_workflow_stdout_path='/work/stdout.log')
        self.mock_paths = Mock(OUTPUT_RESULTS_DIR='/output')

    def test_make_command(self):
        cmd = RunWorkflowCommand(self.mock_job, self.mock_names, self.mock_paths)
        command = cmd.make_command(cwl_base_command=['cwltool'])
        self.assertEqual(command, ['cwltool', '--outdir', '/output', '/work/workflow.cwl', '/work/job-order.json'])

    @patch('lando.common.commands.os')
    @patch('lando.common.commands.subprocess')
    def test_run_post_process_command(self, mock_subprocess, mock_os):
        cmd = RunWorkflowCommand(self.mock_job, self.mock_names, self.mock_paths)
        cmd.run_post_process_command(['rm', 'junk.txt'])
        mock_os.chdir.assert_has_calls([
            call('/output'),
            call(mock_os.getcwd.return_value)
        ])
        mock_subprocess.call.assert_called_with(['rm', 'junk.txt'])

    def test_write_usage_report(self):
        cmd = RunWorkflowCommand(self.mock_job, self.mock_names, self.mock_paths)
        cmd.write_json_file = Mock()
        cmd.write_usage_report(started=parse("2019-01-01T12:30:00"), finished=parse("2019-01-01T18:00:00"))
        cmd.write_json_file.assert_called_with('/work/usage.json', {
            "start_time": "2019-01-01T12:30:00",
            "finish_time": "2019-01-01T18:00:00",
        })

    def test_run(self):
        cmd = RunWorkflowCommand(self.mock_job, self.mock_names, self.mock_paths)
        cmd.run_command = Mock()
        cmd.run_command.return_value = Mock(started=parse("2019-01-01T12:30:00"), finished=parse("2019-01-01T18:00:00"))
        cmd.write_usage_report = Mock()
        cmd.run_post_process_command = Mock()
        cmd.run(cwl_base_command=["cwltool"], cwl_post_process_command=["rm", "junk.txt"])

        cmd.run_command.assert_called_with(['cwltool', '--outdir', '/output',
                                            '/work/workflow.cwl', '/work/job-order.json'],
                                           stderr_path='/work/stderr.log', stdout_path='/work/stdout.log')
        cmd.write_usage_report.assert_called_with(parse("2019-01-01T12:30:00"), parse("2019-01-01T18:00:00"))
        cmd.run_post_process_command.assert_called_with(["rm", "junk.txt"])


class OrganizeOutputCommandTestCase(TestCase):
    def setUp(self):
        self.mock_job = Mock()
        self.mock_job.id = '123'
        self.mock_job.workflow.workflow_type = 'zipped'
        self.mock_names = Mock(usage_report_path='/tmp/usage.json',
                               run_workflow_stderr_path='/tmp/stderr.log',
                               run_workflow_stdout_path='/tmp/stdout.log',
                               workflow_download_dest='/data/workflow.cwl',
                               job_order_path='/data/job-order.json',
                               workflow_to_read='/data/workflow.cwl',
                               organize_output_command_filename='/config/cmd.json')
        self.mock_paths = Mock(OUTPUT_RESULTS_DIR='/results')

    def test_command_file_dict(self):
        cmd = OrganizeOutputCommand(self.mock_job, self.mock_names, self.mock_paths)
        command_file_dict = cmd.command_file_dict(methods_document_content="#stuff")
        self.assertEqual(command_file_dict, {
            "bespin_job_id": '123',
            "destination_dir": '/results',
            "downloaded_workflow_path": '/data/workflow.cwl',
            "workflow_to_read": '/data/workflow.cwl',
            "workflow_type": 'zipped',
            "job_order_path": '/data/job-order.json',
            "bespin_workflow_stdout_path": '/tmp/stdout.log',
            "bespin_workflow_stderr_path": '/tmp/stderr.log',
            "methods_template": "#stuff",
            "additional_log_files": [
                '/tmp/usage.json'
            ]
        })

    def test_run(self):
        cmd = OrganizeOutputCommand(self.mock_job, self.mock_names, self.mock_paths)
        cmd.write_json_file = Mock()
        cmd.run_command = Mock()
        cmd.run(base_command=['organizeit'], methods_document_content="#stuff")
        cmd.write_json_file.assert_has_calls([
            call('/config/cmd.json', ANY),
        ])
        cmd.run_command.assert_called_with(['organizeit', '/config/cmd.json'])


class SaveOutputCommandTestCase(TestCase):
    def setUp(self):
        self.mock_names = Mock(output_project_name='myproject',
                               workflow_input_files_metadata_path='/data/metadata.json',
                               run_workflow_stdout_path='/data/workflow.log',
                               save_output_command_filename='/config/cmd.json',
                               dds_config_filename='/config/ddsclient.conf',
                               output_project_details_filename='project_details.txt')
        self.mock_paths = Mock(REMOTE_README_FILE_PATH='/data/readme.md', OUTPUT_RESULTS_DIR='/results')

    def test_command_file_dict(self):
        cmd = SaveOutputCommand(self.mock_names, self.mock_paths,
                                activity_name="myactivity", activity_description='myactivitydesc')
        command_file_dict = cmd.command_file_dict(share_dds_ids=['123'], started_on='start', ended_on='end')
        self.assertEqual(command_file_dict, {
            "destination": 'myproject',
            "readme_file_path": '/data/readme.md',
            "paths": ['/results'],
            "share": {
                "dds_user_ids": ['123']
            },
            "activity": {
                "name": "myactivity",
                "description": "myactivitydesc",
                "started_on": 'start',
                "ended_on": 'end',
                "input_file_versions_json_path": '/data/metadata.json',
                "workflow_output_json_path": '/data/workflow.log'
            }
        })

    def test_run(self):
        cmd = SaveOutputCommand(self.mock_names, self.mock_paths,
                                activity_name="myactivity", activity_description='myactivitydesc')
        cmd.write_json_file = Mock()
        cmd.write_dds_config_file = Mock()
        cmd.run_command_with_dds_env = Mock()
        dds_credentials = Mock()
        cmd.run(['uploadit'], dds_credentials, ["123"], started_on="start", ended_on="end")

        cmd.write_json_file.assert_called_with('/config/cmd.json', ANY)
        cmd.write_dds_config_file.assert_called_with('/config/ddsclient.conf', ANY)
        cmd.run_command_with_dds_env.assert_called_with(
            ['uploadit', '/config/cmd.json', 'project_details.txt', '--outfile-format', 'json'],
            '/config/ddsclient.conf')
