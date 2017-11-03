from __future__ import absolute_import
from unittest import TestCase
import os
import tempfile
import shutil
from lando.testutil import text_to_file, file_to_text
from lando.worker.cwlworkflow import CwlWorkflow, RESULTS_DIRECTORY
from lando.worker.cwlworkflow import CwlDirectory, CwlWorkflowProcess, ResultsDirectory, JOB_STDERR_OUTPUT_MAX_LINES
from mock import patch, MagicMock, call
from lando.exceptions import JobStepFailed

SAMPLE_WORKFLOW = """
{
    "cwlVersion": "v1.0",
    "class": "CommandLineTool",
    "baseCommand": "cat",
    "stdout": "$(inputs.outputfile)",
    "inputs": [
        {
            "type": {
                "type": "array",
                "items": "File"
            },
            "inputBinding": {
                "position": 1
            },
            "id": "#main/files"
        }
    ],
    "outputs": [
        {
            "type": "File",
            "id": "#main/outputfile",
            "outputBinding": {
                "glob": "$(inputs.outputfile)"
            }
        }
    ],
    "id": "#main"
}
"""


class TestCwlWorkflow(TestCase):
    def make_input_files(self, input_file_directory):
        one_path = os.path.join(input_file_directory, 'one.txt')
        text_to_file("one\n", one_path)
        two_path = os.path.join(input_file_directory, 'two.txt')
        text_to_file("two\n", two_path)
        return one_path, two_path

    def test_simple_workflow_succeeds(self):
        """
        Tests a simple cwl workflow to make sure CwlWorkflow connects all inputs/outputs correctly.
        """
        job_id = 1
        working_directory = tempfile.mkdtemp()
        cwl_base_command = None
        cwl_post_process_command = None
        workflow_directory = tempfile.mkdtemp()
        cwl_path = os.path.join(workflow_directory, 'workflow.cwl')
        text_to_file(SAMPLE_WORKFLOW, cwl_path)
        cwl_file_url = "file://{}".format(cwl_path)
        workflow_object_name = ""

        input_file_directory = tempfile.mkdtemp()
        one_path, two_path = self.make_input_files(input_file_directory)
        job_order = """
files:
  - class: File
    path: {}
  - class: File
    path: {}
outputfile: results.txt
        """.format(one_path, two_path)
        workflow = CwlWorkflow(job_id,
                               working_directory,
                               cwl_base_command,
                               cwl_post_process_command,
                               '# Workflow Methods Markdown')
        workflow.run(cwl_file_url, workflow_object_name, job_order)
        shutil.rmtree(workflow_directory)
        shutil.rmtree(input_file_directory)

        result_file_content = file_to_text(os.path.join(working_directory,
                                                        'working',
                                                        RESULTS_DIRECTORY,
                                                        "results.txt"))
        self.assertEqual(result_file_content, "one\ntwo\n")
        shutil.rmtree(working_directory)

    def test_simple_workflow_missing_files(self):
        """
        Make sure run raises JobStepFailed when we are missing input files.
        """
        job_id = 1
        working_directory = tempfile.mkdtemp()
        cwl_base_command = None
        cwl_post_process_command = None
        workflow_directory = tempfile.mkdtemp()
        cwl_path = os.path.join(workflow_directory, 'workflow.cwl')
        text_to_file(SAMPLE_WORKFLOW, cwl_path)
        cwl_file_url = "file://{}".format(cwl_path)
        workflow_object_name = ""

        input_file_directory = tempfile.mkdtemp()
        one_path, two_path = self.make_input_files(input_file_directory)
        job_order = """
        files:
          - class: File
            path: {}
          - class: File
            path: {}
        outputfile: results.txt
                """.format(one_path, two_path)
        os.unlink(one_path)
        os.unlink(two_path)
        workflow = CwlWorkflow(job_id,
                               working_directory,
                               cwl_base_command,
                               cwl_post_process_command,
                               "# markdown")
        with self.assertRaises(JobStepFailed):
            workflow.run(cwl_file_url, workflow_object_name, job_order)
        shutil.rmtree(workflow_directory)
        shutil.rmtree(input_file_directory)
        shutil.rmtree(working_directory)

    @patch("lando.worker.cwlworkflow.CwlDirectory")
    @patch("lando.worker.cwlworkflow.CwlWorkflowProcess")
    @patch("lando.worker.cwlworkflow.ResultsDirectory")
    def test_workflow_bad_exit_status(self, mock_results_directory, mock_cwl_workflow_process, mock_cwl_directory):
        process_instance = mock_cwl_workflow_process.return_value
        process_instance.return_code = 127
        process_instance.error_output = '1\n2\n3\n4\n5\n6\n7\n8\n9\n10'
        expected_error_message = "CWL workflow failed with exit code: 127\n8\n9\n10"
        job_id = '123'
        working_directory = '/tmp/job_123'
        cwl_base_command = 'cwl-runner'
        cwl_post_process_command = None
        cwl_file_url = 'file://packed.cwl'
        workflow_object_name = '#main'
        job_order = {}
        workflow = CwlWorkflow(job_id, working_directory, cwl_base_command, cwl_post_process_command, "# markdown")
        self.assertEqual(workflow.max_stderr_output_lines, JOB_STDERR_OUTPUT_MAX_LINES)
        workflow.max_stderr_output_lines = 3
        with self.assertRaises(JobStepFailed) as raised_error:
            workflow.run(cwl_file_url, workflow_object_name, job_order)
        self.assertEqual(expected_error_message, raised_error.exception.value)


class TestCwlDirectory(TestCase):
    @patch("lando.worker.cwlworkflow.save_data_to_directory")
    @patch("lando.worker.cwlworkflow.create_dir_if_necessary")
    @patch("lando.worker.cwlworkflow.urllib")
    def test_constructor(self, mock_urllib, mock_create_dir_if_necessary, mock_save_data_to_directory):
        mock_save_data_to_directory.return_value = 'somepath'
        working_directory = '/tmp/fakedir/'
        cwl_file_url = 'file://tmp/notreal.cwl'
        job_order = '{}'

        cwl_directory = CwlDirectory(3, working_directory, cwl_file_url, job_order)

        self.assertEqual(working_directory, cwl_directory.working_directory)
        self.assertEqual('/tmp/fakedir/working', cwl_directory.result_directory)
        mock_create_dir_if_necessary.assert_called_with('/tmp/fakedir/working')
        self.assertEqual('/tmp/fakedir/notreal.cwl', cwl_directory.workflow_path)
        self.assertEqual('somepath', cwl_directory.job_order_file_path)


class TestCwlWorkflowProcess(TestCase):
    @patch("lando.worker.cwlworkflow.os.mkdir")
    def test_run_stdout_good_exit(self, mock_mkdir):
        """
        Swap out cwl-runner for echo and check output
        """
        process = CwlWorkflowProcess(cwl_base_command=['echo'],
                                     local_output_directory='outdir',
                                     workflow_file='workflow',
                                     job_order_filename='joborder')
        process.run()
        self.assertEqual(0, process.return_code)
        absolute_output_dir = os.path.abspath('outdir')
        self.assertEqual("--outdir {} workflow joborder".format(absolute_output_dir), process.output.strip())

    @patch("lando.worker.cwlworkflow.os.mkdir")
    def test_run_stderr_bad_exit(self, mock_mkdir):
        """
        Testing that CwlWorkflowProcess traps stderr and the bad exit code.
        Swap out cwl-runner for bogus ddsclient call that should fail.
        ddsclient is installed for use as a module in staging.
        """
        process = CwlWorkflowProcess(cwl_base_command=['ddsclient'],
                                     local_output_directory='outdir',
                                     workflow_file='workflow',
                                     job_order_filename='joborder')
        process.run()
        self.assertEqual(2, process.return_code)
        self.assertIn("usage", process.error_output.strip())


class TestResultsDirectory(TestCase):
    @patch("lando.worker.cwlworkflow.create_dir_if_necessary")
    @patch("lando.worker.cwlworkflow.save_data_to_directory")
    @patch("lando.worker.cwlworkflow.shutil")
    @patch("lando.worker.cwlworkflow.create_workflow_info")
    @patch("lando.worker.cwlworkflow.CwlReport")
    @patch("lando.worker.cwlworkflow.ScriptsReadme")
    def test_add_files(self, mock_scripts_readme, mock_cwl_report, mock_create_workflow_info, mock_shutil,
                       mock_save_data_to_directory, mock_create_dir_if_necessary):
        job_id = 1
        cwl_directory = MagicMock(result_directory='/tmp/fakedir',
                                  workflow_path='/tmp/nosuchpath.cwl',
                                  workflow_basename='nosuch.cwl',
                                  job_order_file_path='/tmp/alsonotreal.json')
        # Create directory
        results_directory = ResultsDirectory(job_id, cwl_directory, '# Methods Markdown')

        # Make dummy data so we can serialize the values
        mock_create_workflow_info().total_file_size_str.return_value = '1234'
        mock_create_workflow_info().count_output_files.return_value = 1
        cwl_process = MagicMock(output='stdoutdata', error_output='stderrdata')
        cwl_process.started.isoformat.return_value = ''
        cwl_process.finished.isoformat.return_value = ''
        cwl_process.total_runtime_str.return_value = '0 minutes'

        # Specify README data
        mock_cwl_report.return_value.render_markdown.return_value = '#Markdown'
        mock_cwl_report.return_value.render_html.return_value = '<html></html>'
        mock_scripts_readme.return_value.render_markdown.return_value = '#Markdown2'
        mock_scripts_readme.return_value.render_html.return_value = '<html>2</html>'

        # Ask directory to add files based on a mock process
        results_directory.add_files(cwl_process)
        documentation_directory = '/tmp/fakedir/results/docs/'

        mock_create_dir_if_necessary.assert_has_calls([
            call(documentation_directory + "logs"),
            call(documentation_directory + "scripts")])
        mock_save_data_to_directory.assert_has_calls([
            call(documentation_directory + 'logs', 'cwltool-output.json', 'stdoutdata'),
            call(documentation_directory + 'logs', 'cwltool-output.log', 'stderrdata'),
            call('/tmp/fakedir/results', 'Methods.html', '<h1>Methods Markdown</h1>'),
            call('/tmp/fakedir/results/docs', 'README.html', '<html></html>'),
            call('/tmp/fakedir/results/docs', 'README.md', '#Markdown'),
            call('/tmp/fakedir/results/docs/scripts', 'README.html', '<html>2</html>'),
            call('/tmp/fakedir/results/docs/scripts', 'README.md', '#Markdown2'),

        ], any_order=True)
        mock_shutil.copy.assert_has_calls([
            call('/tmp/nosuchpath.cwl', documentation_directory + 'scripts/nosuch.cwl'),
            call('/tmp/alsonotreal.json', documentation_directory + 'scripts/alsonotreal.json')
        ])
        mock_create_workflow_info.assert_has_calls([
            call(workflow_path=(documentation_directory + 'scripts/nosuch.cwl')),
            call().update_with_job_order(job_order_path=(documentation_directory + 'scripts/alsonotreal.json')),
            call().update_with_job_output(job_output_path=(documentation_directory + 'logs/cwltool-output.json')),
            call().count_output_files(),
            call().total_file_size_str()
        ])
