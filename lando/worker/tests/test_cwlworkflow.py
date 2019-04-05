
from unittest import TestCase
import os
import tempfile
import shutil
from lando.testutil import text_to_file, file_to_text
from lando.worker.cwlworkflow import CwlWorkflow, RESULTS_DIRECTORY
from lando.worker.cwlworkflow import CwlDirectory, CwlWorkflowProcess, ResultsDirectory, JOB_STDERR_OUTPUT_MAX_LINES
from lando.worker.cwlworkflow import read_file
from lando.worker.cwlworkflow import CwlWorkflowDownloader
from unittest.mock import patch, MagicMock, call, create_autospec
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
        workflow_type = 'packed'
        workflow.run(workflow_type, cwl_file_url, workflow_object_name, job_order)
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
            workflow.run('packed', cwl_file_url, workflow_object_name, job_order)
        shutil.rmtree(workflow_directory)
        shutil.rmtree(input_file_directory)
        shutil.rmtree(working_directory)


    @patch("lando.worker.cwlworkflow.CwlWorkflowDownloader")
    @patch("lando.worker.cwlworkflow.CwlDirectory")
    @patch("lando.worker.cwlworkflow.CwlWorkflowProcess")
    @patch("lando.worker.cwlworkflow.ResultsDirectory")
    @patch("lando.worker.cwlworkflow.read_file")
    def test_workflow_bad_exit_status(self, mock_read_file, mock_results_directory, mock_cwl_workflow_process, mock_cwl_directory, mock_downloader):
        mock_cwl_directory.return_value.result_directory = '/tmp'
        process_instance = mock_cwl_workflow_process.return_value
        process_instance.return_code = 127
        process_instance.stderr_path = 'stderr.txt'
        expected_error_message = "CWL workflow failed with exit code: 127\n8\n9\n10"
        mock_read_file.return_value =  '1\n2\n3\n4\n5\n6\n7\n8\n9\n10'
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
            workflow.run('packed', cwl_file_url, workflow_object_name, job_order)
        self.assertEqual(expected_error_message, raised_error.exception.value)
        self.assertTrue(mock_read_file.has_call(process_instance.stderr_path))

    @patch("lando.worker.cwlworkflow.subprocess")
    @patch("lando.worker.cwlworkflow.os")
    @patch("lando.worker.cwlworkflow.urllib")
    @patch("lando.worker.cwlworkflow.save_data_to_directory")
    @patch("lando.worker.cwlworkflow.CwlWorkflowProcess")
    @patch("lando.worker.cwlworkflow.ResultsDirectory")
    def test_post_process_command(self, mock_results_directory, mock_cwl_workflow_process, mock_save_data_to_directory,
                                  mock_urllib, mock_os, mock_subprocess):
        """
        Tests a simple cwl workflow to make sure CwlWorkflow connects all inputs/outputs correctly.
        """
        job_id = 1
        cwl_base_command = None
        cwl_post_process_command = ['rm', 'bad-data.dat']
        working_directory = '/fake_working_dir'
        workflow_type = 'packed'
        cwl_file_url = "fakeurl"
        workflow_object_name = ""
        job_order = {}
        mock_cwl_workflow_process.return_value.return_code = 0
        mock_os.getcwd.return_value = "/tmp/working_directory"
        mock_results_directory.return_value.result_directory = "/tmp/output/results"
        workflow = CwlWorkflow(job_id,
                               working_directory,
                               cwl_base_command,
                               cwl_post_process_command,
                               '# Workflow Methods Markdown')
        workflow.run(workflow_type, cwl_file_url, workflow_object_name, job_order)
        mock_subprocess.call.assert_called_with(['rm', 'bad-data.dat'])
        mock_os.chdir.assert_has_calls([
            call("/tmp/output/results"),
            call("/tmp/working_directory"),
        ])


class TestCwlDirectory(TestCase):
    @patch("lando.worker.cwlworkflow.save_data_to_directory")
    @patch("lando.worker.cwlworkflow.create_dir_if_necessary")
    @patch("lando.worker.cwlworkflow.urllib")
    def test_constructor(self, mock_urllib, mock_create_dir_if_necessary, mock_save_data_to_directory):
        mock_save_data_to_directory.return_value = 'somepath'
        working_directory = '/tmp/fakedir/'
        job_order = '{}'
        mock_downloader = create_autospec(CwlWorkflowDownloader)
        cwl_directory = CwlDirectory(3, working_directory, mock_downloader, job_order)
        self.assertEqual(working_directory, cwl_directory.working_directory)
        self.assertEqual('/tmp/fakedir/working', cwl_directory.result_directory)
        self.assertEqual('somepath', cwl_directory.job_order_file_path)
        mock_create_dir_if_necessary.assert_called_with('/tmp/fakedir/working')


class CwlWorkflowDownloaderTestCase(TestCase):

    def setUp(self):
        self.zip_url = 'https://server.com/workflow.zip'
        self.zip_path = 'version-1.0/workflow.cwl'
        self.packed_url = 'https://server.com/packed.cwl'
        self.packed_path = '#main'
        self.working_directory = '/work'

    @patch('lando.worker.cwlworkflow.urllib')
    @patch('lando.worker.cwlworkflow.zipfile.ZipFile')
    def test_downloads_zipped(self, mock_zipfile, mock_urllib):
        downloader = CwlWorkflowDownloader(self.working_directory,
                                           'zipped',
                                           self.zip_url,
                                           self.zip_path)
        self.assertEqual(downloader.workflow_basename, 'workflow.zip')
        self.assertEqual(downloader.workflow_to_run, '/work/version-1.0/workflow.cwl')
        self.assertEqual(downloader.workflow_to_report, 'version-1.0/workflow.cwl')
        self.assertEqual(downloader.download_path, '/work/workflow.zip')
        self.assertEqual(mock_urllib.request.urlretrieve.call_args, call(self.zip_url, downloader.download_path))
        self.assertEqual(mock_zipfile.call_args, call(downloader.download_path))
        self.assertEqual(mock_zipfile.return_value.__enter__.return_value.extractall.call_args, call(self.working_directory))

    @patch('lando.worker.cwlworkflow.urllib.request.urlretrieve')
    @patch('lando.worker.cwlworkflow.zipfile.ZipFile')
    def test_downloads_packed(self, mock_zipfile, mock_urlretrieve):
        downloader = CwlWorkflowDownloader(self.working_directory,
                                           'packed',
                                           self.packed_url,
                                           self.packed_path)
        self.assertEqual(downloader.workflow_basename, 'packed.cwl')
        self.assertEqual(downloader.workflow_to_run, '/work/packed.cwl#main')
        self.assertEqual(downloader.workflow_to_report, 'packed.cwl#main')
        self.assertEqual(downloader.download_path, '/work/packed.cwl')
        self.assertEqual(mock_urlretrieve.call_args, call(self.packed_url, downloader.download_path))
        self.assertFalse(mock_zipfile.called)

    @patch('lando.worker.cwlworkflow.urllib.request.urlretrieve')
    @patch('lando.worker.cwlworkflow.zipfile.ZipFile')
    def test_downloads_then_raises_on_other_type(self, mock_zipfile, mock_urlretrieve):
        other_url = 'http://site.com/file.ext'
        expected_download_path = '/work/file.ext'
        with self.assertRaises(JobStepFailed) as context:
            CwlWorkflowDownloader(self.working_directory, 'other', other_url, '#')
        self.assertIn('Unsupported workflow type', str(context.exception))
        self.assertEqual(mock_urlretrieve.call_args, call(other_url, expected_download_path))
        self.assertFalse(mock_zipfile.called)

    @patch('lando.worker.cwlworkflow.shutil.copy')
    @patch('lando.worker.cwlworkflow.urllib.request.urlretrieve')
    @patch('lando.worker.cwlworkflow.zipfile.ZipFile')
    def test_copy_packed(self, mock_zipfile, mock_urlretrieve, mock_copy):
        downloader = CwlWorkflowDownloader(self.working_directory,
                                           'packed',
                                           self.packed_url,
                                           self.packed_path)
        directory = '/target'
        downloader.copy_to_directory(directory)
        self.assertEqual(mock_copy.call_args, call(downloader.download_path, '/target/packed.cwl'))
        self.assertFalse(mock_zipfile.called)

    @patch('lando.worker.cwlworkflow.shutil.copy')
    @patch('lando.worker.cwlworkflow.urllib.request.urlretrieve')
    @patch('lando.worker.cwlworkflow.zipfile.ZipFile')
    def test_copy_zipped(self, mock_zipfile, mock_urlretrieve, mock_copy):
        downloader = CwlWorkflowDownloader(self.working_directory,
                                           'zipped',
                                           self.zip_url,
                                           self.zip_path)
        mock_zipfile.reset() # Reset so that we can detect the second unzip
        directory = '/target'
        downloader.copy_to_directory(directory)
        self.assertFalse(mock_copy.called)
        self.assertEqual(mock_zipfile.call_args, call(downloader.download_path))
        self.assertEqual(mock_zipfile.return_value.__enter__.return_value.extractall.call_args, call(directory))


class CwlWorkflowDownloaderActivityPathTestCase(TestCase):
    """
    Test for method that provides the path of the workflow file to use in a provenance activity
    """
    def test_get_workflow_activity_path_packed(self):
        # def get_workflow_activity_path(workflow_type, workflow_url, workflow_path):

        path = CwlWorkflowDownloader.get_workflow_activity_path('packed', 'http://example.com/workflow.cwl', '#main')
        self.assertEqual(path, 'workflow.cwl')

    def test_get_workflow_activity_path_zipped(self):
        path = CwlWorkflowDownloader.get_workflow_activity_path('zipped', 'http://example.com/workflow.zip', 'path/workflow.cwl')
        self.assertEqual(path, 'path/workflow.cwl')

    def test_get_workflow_activity_path_raises_other(self):
        with self.assertRaises(JobStepFailed) as context:
            CwlWorkflowDownloader.get_workflow_activity_path('other', None, None)
        self.assertIn('Unsupported workflow type', str(context.exception))


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
                                     job_order_filename='joborder')
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
                                     job_order_filename='joborder')
        process.run()
        self.assertEqual(2, process.return_code)


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
        workflow_downloader = create_autospec(CwlWorkflowDownloader,
                                              workflow_to_run='/tmp/nosuchpath.cwl',
                                              workflow_to_report='nosuch.cwl',
                                              workflow_basename='nosuch.cwl')
        cwl_directory = create_autospec(CwlDirectory,
                                        result_directory='/tmp/fakedir',
                                        job_order_file_path='/tmp/alsonotreal.json',
                                        workflow_downloader=workflow_downloader)

        # Create directory
        results_directory = ResultsDirectory(job_id, cwl_directory, '# Methods Markdown')

        # Make dummy data so we can serialize the values
        mock_create_workflow_info().total_file_size_str.return_value = '1234'
        mock_create_workflow_info().count_output_files.return_value = 1
        cwl_process = MagicMock(stdout_path='/path/to/stdout', stderr_path='/path/to/stderr')
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
            call('/tmp/fakedir/results', 'Methods.html', '<h1>Methods Markdown</h1>'),
            call('/tmp/fakedir/results/docs', 'README.html', '<html></html>'),
            call('/tmp/fakedir/results/docs', 'README.md', '#Markdown'),
            call('/tmp/fakedir/results/docs/scripts', 'README.html', '<html>2</html>'),
            call('/tmp/fakedir/results/docs/scripts', 'README.md', '#Markdown2'),
        ], any_order=True)
        mock_shutil.copy.assert_has_calls([
            call('/path/to/stdout', documentation_directory + 'logs/cwltool-output.json'),
            call('/path/to/stderr', documentation_directory + 'logs/cwltool-output.log'),
            call('/tmp/alsonotreal.json', documentation_directory + 'scripts/alsonotreal.json')
        ])
        workflow_downloader.copy_to_directory.assert_has_calls([
            call(documentation_directory + 'scripts')
        ])
        mock_create_workflow_info.assert_has_calls([
            call(workflow_path=(documentation_directory + 'scripts/nosuch.cwl')),
            call().update_with_job_order(job_order_path=(documentation_directory + 'scripts/alsonotreal.json')),
            call().update_with_job_output(job_output_path=(documentation_directory + 'logs/cwltool-output.json')),
            call().count_output_files(),
            call().total_file_size_str()
        ])

    @patch("lando.worker.cwlworkflow.create_dir_if_necessary")
    @patch("lando.worker.cwlworkflow.save_data_to_directory")
    @patch("lando.worker.cwlworkflow.shutil")
    @patch("lando.worker.cwlworkflow.create_workflow_info")
    @patch("lando.worker.cwlworkflow.CwlReport")
    @patch("lando.worker.cwlworkflow.ScriptsReadme")
    def test_add_files_null_methods_document(self, mock_scripts_readme, mock_cwl_report, mock_create_workflow_info,
                                             mock_shutil, mock_save_data_to_directory, mock_create_dir_if_necessary):
        job_id = 1
        workflow_downloader = create_autospec(CwlWorkflowDownloader,
                                              workflow_to_run='/tmp/nosuchpath.cwl',
                                              workflow_to_report='nosuch.cwl',
                                              workflow_basename='nosuch.cwl')
        cwl_directory = create_autospec(CwlDirectory,
                                        result_directory='/tmp/fakedir',
                                        job_order_file_path='/tmp/alsonotreal.json',
                                        workflow_downloader=workflow_downloader)
        # Create directory
        results_directory = ResultsDirectory(job_id, cwl_directory, None)

        # Make dummy data so we can serialize the values
        mock_create_workflow_info().total_file_size_str.return_value = '1234'
        mock_create_workflow_info().count_output_files.return_value = 1
        cwl_process = MagicMock(stdout_path='/path/to/stdout', stderr_path='/path/to/stderr')
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
            call('/tmp/fakedir/results/docs', 'README.html', '<html></html>'),
            call('/tmp/fakedir/results/docs', 'README.md', '#Markdown'),
            call('/tmp/fakedir/results/docs/scripts', 'README.html', '<html>2</html>'),
            call('/tmp/fakedir/results/docs/scripts', 'README.md', '#Markdown2'),
        ], any_order=True)


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
