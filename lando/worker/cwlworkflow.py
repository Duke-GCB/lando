"""
Runs cwl workflow.
"""

import os
import datetime
import logging
import subprocess
import codecs
from lando.exceptions import JobStepFailed


RUN_CWL_COMMAND = "cwltool"
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


class CwlWorkflow(object):
    """
    Runs a CWL workflow using the cwl-runner command line program.
    1. Writes out job_order to a file
    2. Runs cwl-runner in a separate process
    3. Gathers stderr/stdout output from the process
    4. If exit status is not 0 raises JobStepFailed including output
    """
    def __init__(self, cwl_base_command, cwl_post_process_command, results_directory):
        """
        Setup workflow
        :param cwl_base_command: [str] or None: array of cwl command and arguments (osx requires special arguments)
        :param cwl_post_process_command: [str] or None: post processing command run after cwl_base_command succeeds
        """
        self.cwl_base_command = cwl_base_command
        self.cwl_post_process_command = cwl_post_process_command
        # self.workflow_methods_markdown = workflow_methods_markdown
        self.max_stderr_output_lines = JOB_STDERR_OUTPUT_MAX_LINES
        self.results_directory = results_directory

    def run(self, workflow_to_run, job_order_path, stdout_path, stderr_path):
        process = CwlWorkflowProcess(self.cwl_base_command,
                                     self.results_directory,
                                     workflow_to_run,
                                     job_order_path,
                                     stdout_path, stderr_path)
        process.run()
        if process.return_code != 0:
            stderr_output = read_file(process.stderr_path)
            tail_error_output = self._tail_stderr_output(stderr_output)
            error_message = "CWL workflow failed with exit code: {}\n{}".format(process.return_code, tail_error_output)
            stdout_output = read_file(process.stdout_path)
            raise JobStepFailed(error_message, stdout_output)

        if self.cwl_post_process_command:
            original_directory = os.getcwd()
            os.chdir(self.results_directory)
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
    def __init__(self, cwl_base_command, local_output_directory, workflow_file, job_order_filename,
                 stdout_path, stderr_path):
        """
        Setup to run cwl workflow using the supplied parameters.
        :param cwl_base_command:  [str] or None: array of cwl command and arguments (osx requires special arguments)
        :param local_output_directory: str: path to directory we will save output files into
        :param workflow_file: str: path to the cwl workflow
        :param job_order_filename: str: path to the cwl job order (input file)
        """
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
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
            os.makedirs(self.absolute_output_directory, exist_ok=True)
        self.started = datetime.datetime.now()
        # Configure the supbrocess to write stdout and stderr directly to files
        logging.info('Running command: {}'.format(' '.join(self.command)))
        logging.info('Redirecting stdout > {},  stderr > {}'.format(self.stdout_path, self.stderr_path))
        stdout_file = open(self.stdout_path, 'w')
        stderr_file = open(self.stderr_path, 'w')
        print("Running", self.command)
        try:
            self.return_code = subprocess.call(self.command, stdout=stdout_file, stderr=stderr_file)
        except OSError as e:
            logging.error('Error running subprocess %s', e)
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
