
from unittest import TestCase
import os
import tempfile
import shutil
from lando.testutil import text_to_file, file_to_text
from lando.worker.cwlworkflow import CwlWorkflow, RESULTS_DIRECTORY
from lando.worker.cwlworkflow import CwlWorkflowProcess, JOB_STDERR_OUTPUT_MAX_LINES
from lando.worker.cwlworkflow import read_file
from unittest.mock import patch, MagicMock, call, create_autospec, mock_open
from lando.exceptions import JobStepFailed


class TestCwlWorkflow(TestCase):
    @patch('lando.worker.cwlworkflow.os')
    @patch('lando.worker.cwlworkflow.read_file')
    @patch('lando.worker.cwlworkflow.CwlWorkflowProcess')
    @patch('lando.worker.cwlworkflow.subprocess')
    def test_workflow_succeeds(self, mock_subprocess, mock_cwl_workflow_process, mock_read_file, mock_os):
        mock_cwl_workflow_process.return_value.return_code = 0

        workflow = CwlWorkflow(cwl_base_command=['cwltool'],
                               cwl_post_process_command=None,
                               results_directory='/work/results')
        workflow.run(workflow_to_run='/work/exomseq.cwl', job_order_path='/work/job-order.json',
                     stdout_path='/tmp/stdout.log', stderr_path='/tmp/stderr.log')
        mock_subprocess.call.assert_not_called()


    @patch('lando.worker.cwlworkflow.os')
    @patch('lando.worker.cwlworkflow.read_file')
    @patch('lando.worker.cwlworkflow.CwlWorkflowProcess')
    @patch('lando.worker.cwlworkflow.subprocess')
    def test_workflow_fails(self, mock_subprocess, mock_cwl_workflow_process, mock_read_file, mock_os):
        mock_read_file.return_value = 'some error'
        mock_cwl_workflow_process.return_value.return_code = 1
        workflow = CwlWorkflow(cwl_base_command=['cwltool'],
                               cwl_post_process_command=['rm', 'junk.txt'],
                               results_directory='/work/results')
        with self.assertRaises(JobStepFailed) as raised_exception:
            workflow.run(workflow_to_run='/work/exomseq.cwl', job_order_path='/work/job-order.json',
                         stdout_path='/tmp/stdout.log', stderr_path='/tmp/stderr.log')
        self.assertTrue('CWL workflow failed with exit code: 1' in str(raised_exception.exception))
        self.assertTrue('some error' in str(raised_exception.exception))
        mock_subprocess.call.assert_not_called()


    @patch('lando.worker.cwlworkflow.os')
    @patch('lando.worker.cwlworkflow.read_file')
    @patch('lando.worker.cwlworkflow.CwlWorkflowProcess')
    @patch('lando.worker.cwlworkflow.subprocess')
    def test_post_process_command(self, mock_subprocess, mock_cwl_workflow_process, mock_read_file, mock_os):
        mock_cwl_workflow_process.return_value.return_code = 0
        workflow = CwlWorkflow(cwl_base_command=['cwltool'],
                               cwl_post_process_command=['rm', 'junk.txt'],
                               results_directory='/work/results')
        workflow.run(workflow_to_run='/work/exomseq.cwl', job_order_path='/work/job-order.json',
                     stdout_path='/tmp/stdout.log', stderr_path='/tmp/stderr.log')
        mock_subprocess.call.assert_called_with(['rm', 'junk.txt'])
        mock_os.chdir.assert_has_calls([
            call('/work/results'),  # cd to results directory
            call(mock_os.getcwd.return_value)  # go back to original directory
        ])


class TestCwlWorkflowProcess(TestCase):
    @patch("lando.worker.cwlworkflow.os.mkdir")
    @patch("lando.worker.cwlworkflow.open")
    @patch("lando.worker.cwlworkflow.subprocess")
    def test_run_stdout_good_exit(self, mock_subprocess, mock_open, mock_mkdir):
        """
        Swap out cwl-runner for echo and check output
        """
        mock_subprocess.call.return_value = 0
        process = CwlWorkflowProcess(cwl_base_command=['echo'],
                                     local_output_directory='outdir',
                                     workflow_file='workflow',
                                     job_order_filename='joborder',
                                     stdout_path='/tmp/ddsclient.stdout',
                                     stderr_path='/tmp/ddsclient.stderr')
        process.run()
        self.assertEqual(0, process.return_code)

    @patch("lando.worker.cwlworkflow.os.mkdir")
    @patch("lando.worker.cwlworkflow.open")
    @patch("lando.worker.cwlworkflow.subprocess")
    def test_run_stderr_bad_exit(self, mock_subprocess, mock_open, mock_mkdir):
        """
        Testing that CwlWorkflowProcess traps the bad exit code.
        Swap out cwl-runner for bogus ddsclient call that should fail.
        ddsclient is installed for use as a module in staging.
        """
        mock_subprocess.call.return_value = 2
        process = CwlWorkflowProcess(cwl_base_command=['ddsclient'],
                                     local_output_directory='outdir',
                                     workflow_file='workflow',
                                     job_order_filename='joborder',
                                     stdout_path='/tmp/ddsclient.stdout',
                                     stderr_path='/tmp/ddsclient.stderr')
        process.run()
        self.assertEqual(2, process.return_code)


class TestReadFile(TestCase):

    @patch('lando.worker.cwlworkflow.codecs')
    def test_reads_file_using_codecs(self, mock_codecs):
        expected_contents = 'Contents'
        mock_codecs.open.return_value.__enter__.return_value.read.return_value = expected_contents
        contents = read_file('myfile.txt')
        self.assertEqual(expected_contents, contents)
        mock_codecs.open.assert_called_with('myfile.txt','r',encoding='utf-8',errors='xmlcharrefreplace')

    @patch('lando.worker.cwlworkflow.codecs')
    def test_returns_empty_string_on_error(self, mock_codecs):
        mock_codecs.open.side_effect = OSError()
        contents = read_file('myfile.txt')
        self.assertEqual('', contents)
