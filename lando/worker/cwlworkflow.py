"""
Runs cwl workflow.
"""
from __future__ import print_function
import os
import shutil
import urllib
import datetime
import json
import markdown
import logging
import subprocess
import codecs
from lando.exceptions import JobStepFailed
from lando.worker.cwlreport import create_workflow_info, CwlReport
from lando.worker.scriptsreadme import ScriptsReadme


RUN_CWL_COMMAND = "cwl-runner"
RUN_CWL_OUTDIR_ARG = "--outdir"

RESULTS_DIRECTORY = 'results'
DOCUMENTATION_DIRECTORY = 'docs'
README_MARKDOWN_FILENAME = 'README.md'
README_HTML_FILENAME = 'README.html'

LOGS_DIRECTORY = 'logs'
JOB_STDOUT_FILENAME = 'cwltool-output.json'
JOB_STDERR_FILENAME = 'cwltool-output.log'
JOB_DATA_FILENAME = 'job-data.json'
METHODS_DOCUMENT_FILENAME = 'Methods.html'

WORKFLOW_DIRECTORY = 'scripts'

CWL_WORKING_DIRECTORY = 'working'

JOB_STDERR_OUTPUT_MAX_LINES = 100


def create_dir_if_necessary(path):
    """
    Create a directory if one doesn't already exist.
    :param path: str: path to create a directory.
    """
    if not os.path.exists(path):
        os.mkdir(path)


def build_file_name(directory_path, filename):
    return os.path.join(directory_path, filename)


def save_data_to_directory(directory_path, filename, data):
    """
    Save data into a file at directory_path/filename
    :param directory_path: str: path to directory that should already exist
    :param filename: str: name of the file we will create
    :param data: str: data to be written tothe file
    :return: str: directory_path/filename
    """
    file_path = os.path.join(directory_path, filename)
    with codecs.open(file_path, 'w', encoding='utf-8', errors='xmlcharrefreplace') as outfile:
        outfile.write(data)
    return file_path


def read_file(file_path):
    """
    Read the contents of a file using utf-8 encoding, or return an empty string
    if it does not exist
    :param file_path: str: path to the file to read
    :return: str: contents of file
    """
    try:
        with codecs.open(file_path, 'r', encoding='utf-8', errors='xmlcharrefreplace') as infile:
            return infile.read()
    except OSError as e:
        logging.exception('Error opening {}'.format(file_path))
        return ''


class CwlDirectory(object):
    """
    Creates a directory structure used to run the cwl workflow.
    Layout:
    working_directory/    # base directory for this job
      ...files downloaded during stage in job step
      workflow.cwl        # cwl workflow we will run
      workflow.yml        # job order input file
      <upload_directory>/   # user specified directory name
        results/          # output_directory member
           ...output files from workflow
    """
    def __init__(self, job_id, working_directory, cwl_file_url, job_order):
        """
        :param job_id: int: job id we are running a workflow for
        :param working_directory: str: path to directory cwl will be run in (data files may be relative to this path)
        :param cwl_file_url: str: url to packed cwl file to run
        :param job_order: str: job order string data (JSON or YAML format)
        """
        self.job_id = job_id
        self.working_directory = working_directory
        self.result_directory = os.path.join(working_directory, CWL_WORKING_DIRECTORY)
        create_dir_if_necessary(self.result_directory)
        self.workflow_basename = os.path.basename(cwl_file_url)
        self.workflow_path = self._add_workflow_file(cwl_file_url)
        job_order_filename = self._get_job_order_filename(job_id)
        self.job_order_file_path = save_data_to_directory(self.working_directory, job_order_filename, job_order)

    def _add_workflow_file(self, cwl_file_url):
        """
        Download a packed cwl workflow file.
        :param cwl_file_url: str: url that points to a packed cwl workflow
        :return: str: location we downloaded the to
        """
        workflow_file = os.path.join(self.working_directory, self.workflow_basename)
        urllib.urlretrieve(cwl_file_url, workflow_file)
        return workflow_file

    @staticmethod
    def _get_job_order_filename(job_id):
        """
        Return the filename that will contain the job input settings.
        """
        return "job-{}-input.yml".format(job_id)


class CwlWorkflow(object):
    """
    Runs a CWL workflow using the cwl-runner command line program.
    1. Writes out job_order to a file
    2. Runs cwl-runner in a separate process
    3. Gathers stderr/stdout output from the process
    4. If exit status is not 0 raises JobStepFailed including output
    """
    def __init__(self, job_id, working_directory, cwl_base_command, cwl_post_process_command, workflow_methods_markdown):
        """
        Setup workflow
        :param job_id: int: job id we are running a workflow for
        :param working_directory: str: path to working directory that contains input files
        :param cwl_base_command: [str] or None: array of cwl command and arguments (osx requires special arguments)
        :param cwl_post_process_command: [str] or None: post processing command run after cwl_base_command succeeds
        :param workflow_methods_markdown: str: markdown about the methods used in this workflow
        """
        self.job_id = job_id
        self.working_directory = working_directory
        self.cwl_base_command = cwl_base_command
        self.cwl_post_process_command = cwl_post_process_command
        self.workflow_methods_markdown = workflow_methods_markdown
        self.max_stderr_output_lines = JOB_STDERR_OUTPUT_MAX_LINES

    def run(self, cwl_file_url, workflow_object_name, job_order):
        """
        Downloads the packed cwl workflow from cwl_file_url, runs it.
        If cwl-runner doesn't exit with 0 raise JobStepFailed
        :param cwl_file_url: str: url to workflow we will run (should be packed)
        :param workflow_object_name: name of the object in our workflow to execute (typically '#main')
        :param job_order: str: json string of input parameters for our workflow
        """
        cwl_directory = CwlDirectory(self.job_id, self.working_directory, cwl_file_url, job_order)
        workflow_file = cwl_directory.workflow_path
        if workflow_object_name:
            workflow_file += workflow_object_name
        process = CwlWorkflowProcess(self.cwl_base_command,
                                     os.path.join(cwl_directory.result_directory, RESULTS_DIRECTORY),
                                     workflow_file,
                                     cwl_directory.job_order_file_path)
        process.run()
        if process.return_code != 0:
            stderr_output = read_file(process.stderr_path)
            tail_error_output = self._tail_stderr_output(stderr_output)
            error_message = "CWL workflow failed with exit code: {}\n{}".format(process.return_code, tail_error_output)
            stdout_output = read_file(process.stdout_path)
            raise JobStepFailed(error_message, stdout_output)
        results_directory = ResultsDirectory(self.job_id, cwl_directory, self.workflow_methods_markdown)
        results_directory.add_files(process)
        if self.cwl_post_process_command:
            original_directory = os.getcwd()
            os.chdir(results_directory.result_directory)
            subprocess.call(self.cwl_post_process_command)
            os.chdir(original_directory)

    def _tail_stderr_output(self, stderr_data):
        """
        Trim stderr data to the last JOB_STDERR_OUTPUT_MAX_LINES lines
        :param stderr_data: str: stderr data to be trimmed
        :return: str
        """
        lines = stderr_data.splitlines()
        last_lines = lines[-self.max_stderr_output_lines:]
        return '\n'.join(last_lines)


class CwlWorkflowProcess(object):
    def __init__(self, cwl_base_command, local_output_directory, workflow_file, job_order_filename):
        """
        Setup to run cwl workflow using the supplied parameters.
        :param cwl_base_command:  [str] or None: array of cwl command and arguments (osx requires special arguments)
        :param local_output_directory: str: path to directory we will save output files into
        :param workflow_file: str: path to the cwl workflow
        :param job_order_filename: str: path to the cwl job order (input file)
        """
        self.stdout_path = JOB_STDOUT_FILENAME
        self.stderr_path = JOB_STDERR_FILENAME
        self.return_code = None
        self.started = None
        self.finished = None
        base_command = cwl_base_command
        if not base_command:
            base_command = [RUN_CWL_COMMAND]
        self.command = base_command[:]
        # cwltoil requires an absolute path for output directory
        self.absolute_output_directory = os.path.abspath(local_output_directory)
        self.command.extend([RUN_CWL_OUTDIR_ARG, self.absolute_output_directory, workflow_file, job_order_filename])

    def run(self):
        """
        Run job, writing output to stdout_path/stderr_path, and setting return_code.
        :param command: [str]: array of strings representing a workflow command and its arguments
        """
        # Create output directory for workflow results
        if not os.path.exists(self.absolute_output_directory):
            os.mkdir(self.absolute_output_directory)
        self.started = datetime.datetime.now()
        # Configure the supbrocess to write stdout and stderr directly to files
        logging.info('Running command: {}'.format(' '.join(self.command)))
        logging.info('Redirecting stdout > {},  stderr > {}'.format(self.stdout_path, self.stderr_path))
        stdout_file = open(self.stdout_path, 'w')
        stderr_file = open(self.stderr_path, 'w')
        try:
            self.return_code = subprocess.call(self.command, stdout=stdout_file, stderr=stderr_file)
        except OSError as e:
            logging.error('Error running subprocess', e)
            error_message = "Command failed: {}".format(' '.join(self.command))
            raise JobStepFailed(error_message, e)
        finally:
            stdout_file.close()
            stderr_file.close()
        self.finished = datetime.datetime.now()

    def total_runtime_str(self):
        """
        Returns a string describing how long the job took.
        :return: str: "<number> minutes"
        """
        elapsed_seconds = (self.finished - self.started).total_seconds()
        return "{} minutes".format(elapsed_seconds / 60)


class ResultsDirectory(object):
    """
    Adds resulting files to a CwlDirectory wrapping up workflow input files and results.

    Fills in the following directory structure:
    working_directory/            # base directory for this job
        results/           # this directory is uploaded in the store output stage
           Methods.md      # document detailing methods used in workflow
           ...output files from workflow
           docs/
              README         # describes contents of the upload_directory
              logs/
                  cwltool-output.json   #stdout from cwl-runner - json job results
                  cwltool-output.log    #stderr from cwl-runner
                  job-data.json         # non-cwl job data used to create Bespin-Report.txt
              workflow/
                  workflow.cwl            # cwl workflow we will run
                  workflow.yml            # job order input file
    """
    def __init__(self, job_id, cwl_directory, workflow_methods_markdown_content):
        """
        :param job_id: int: job id associated with this job
        :param cwl_directory: CwlDirectory: directory data for a job that has been run
        :param workflow_methods_markdown_content: str: markdown
        """
        self.job_id = job_id
        self.result_directory = os.path.join(cwl_directory.result_directory, RESULTS_DIRECTORY)
        create_dir_if_necessary(self.result_directory)
        self.docs_directory = os.path.join(self.result_directory, DOCUMENTATION_DIRECTORY)
        create_dir_if_necessary(self.docs_directory)
        self.workflow_path = cwl_directory.workflow_path
        self.workflow_basename = cwl_directory.workflow_basename
        self.job_order_file_path = cwl_directory.job_order_file_path
        self.job_order_filename = os.path.basename(cwl_directory.job_order_file_path)
        self.workflow_methods_markdown_content = workflow_methods_markdown_content

    def add_files(self, cwl_process):
        """
        Add output files to the resulting directory based on the finished process.
        :param cwl_process: CwlProcess: process that was run - contains stdout, stderr, and exit status
        """
        self._copy_log_files(cwl_process.stdout_path, cwl_process.stderr_path)
        self._copy_workflow_inputs()
        self._create_report(cwl_process)
        self._create_running_instructions()
        self._add_methods_document()

    def _copy_log_files(self, output_log_path, error_log_path):
        """
        Copy stdout and stderr log files to the 'logs' directory.
        :param output_log_path: str: Path to file containing stdout from cwl-runner
        :param error_log_path: str: Path to file containing stderr from cwl-runner
        """
        logs_directory = os.path.join(self.docs_directory, LOGS_DIRECTORY)
        create_dir_if_necessary(logs_directory)
        shutil.copy(output_log_path, os.path.join(logs_directory, JOB_STDOUT_FILENAME))
        shutil.copy(error_log_path, os.path.join(logs_directory, JOB_STDERR_FILENAME))

    def _copy_workflow_inputs(self):
        """
        Copies workflow input files to the 'workflow' directory.
        """
        workflow_directory = os.path.join(self.docs_directory, WORKFLOW_DIRECTORY)
        create_dir_if_necessary(workflow_directory)
        shutil.copy(self.workflow_path, os.path.join(workflow_directory, self.workflow_basename))
        shutil.copy(self.job_order_file_path, os.path.join(workflow_directory, self.job_order_filename))

    def _create_report(self, cwl_process):
        """
        Creates a report to the directory that will be uploaded based on the inputs and outputs of the workflow.
        Also saves the json job-specific file into logs.
        :param cwl_process: CwlProcess: contains job start/stop info
        """
        logs_directory = os.path.join(self.docs_directory, LOGS_DIRECTORY)
        workflow_directory = os.path.join(self.docs_directory, WORKFLOW_DIRECTORY)
        workflow_info = create_workflow_info(workflow_path=os.path.join(workflow_directory, self.workflow_basename))
        workflow_info.update_with_job_order(job_order_path=os.path.join(workflow_directory, self.job_order_filename))
        workflow_info.update_with_job_output(job_output_path=os.path.join(logs_directory, JOB_STDOUT_FILENAME))
        job_data = {
            'id': self.job_id,
            'started': cwl_process.started.isoformat(),
            'finished': cwl_process.finished.isoformat(),
            'run_time': cwl_process.total_runtime_str(),
            'num_output_files': workflow_info.count_output_files(),
            'total_file_size_str': workflow_info.total_file_size_str(),
            'workflow_methods': self.workflow_methods_markdown_content
        }
        report = CwlReport(workflow_info, job_data)
        save_data_to_directory(self.docs_directory, README_MARKDOWN_FILENAME, report.render_markdown())
        save_data_to_directory(self.docs_directory, README_HTML_FILENAME, report.render_html())
        self._save_job_data(job_data)

    def _save_job_data(self, job_data):
        """
        Save job data using in building the report into a JSON file under logs.
        :param job_data: dict: non-cwl values used in the Report
        """
        logs_directory = os.path.join(self.docs_directory, LOGS_DIRECTORY)
        save_data_to_directory(logs_directory, JOB_DATA_FILENAME, json.dumps(job_data))

    def _create_running_instructions(self):
        workflow_directory = os.path.join(self.docs_directory, WORKFLOW_DIRECTORY)
        scripts_readme = ScriptsReadme(self.workflow_basename, self.job_order_filename)
        save_data_to_directory(workflow_directory, README_MARKDOWN_FILENAME, scripts_readme.render_markdown())
        save_data_to_directory(workflow_directory, README_HTML_FILENAME, scripts_readme.render_html())

    def _add_methods_document(self):
        html = markdown.markdown(self.workflow_methods_markdown_content)
        save_data_to_directory(self.result_directory, METHODS_DOCUMENT_FILENAME, html)
