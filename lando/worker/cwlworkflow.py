"""
Runs cwl workflow.
"""
from __future__ import print_function
import os
import shutil
import urllib
import datetime
from subprocess import PIPE, Popen
from lando.exceptions import JobStepFailed
from lando.worker.cwlreport import create_workflow_info, CwlReport


RUN_CWL_COMMAND = "cwl-runner"
RUN_CWL_OUTDIR_ARG = "--outdir"

REPORT_FILENAME = 'Bespin-Report.txt'

LOGS_DIRECTORY = 'logs'
JOB_STDOUT_FILENAME = 'cwltool-output.json'
JOB_STDERR_FILENAME = 'cwltool-output.log'

WORKFLOW_DIRECTORY = 'workflow'
WORKFLOW_FILENAME = 'workflow.cwl'
JOB_ORDER_FILENAME = 'workflow.yml'

RESULTS_DIRECTORY_FILENAME = 'results'


def create_dir_if_necessary(path):
    """
    Create a directory if one doesn't already exist.
    :param path: str: path to create a directory.
    """
    if not os.path.exists(path):
        os.mkdir(path)


def save_data_to_directory(directory_path, filename, data):
    """
    Save data into a file at directory_path/filename
    :param directory_path: str: path to directory that should already exist
    :param filename: str: name of the file we will create
    :param data: str: data to be written tothe file
    :return: str: directory_path/filename
    """
    file_path = os.path.join(directory_path, filename)
    with open(file_path, 'w') as outfile:
        outfile.write(data)
    return file_path


class CwlDirectory(object):
    """
    Creates a directory structure used to run the cwl workflow.
    Layout:
    working_directory/    # base directory for this job
      ...files downloaded during stage in job step
      workflow.cwl        # cwl workflow we will run
      workflow.yml        # job order input file
      upload_directory/   # this is a user specified name
        results/
           ...output files from workflow
    """
    def __init__(self, working_directory, user_directory_name, cwl_file_url, job_order):
        self.working_directory = working_directory
        self.base_directory = os.path.join(working_directory, user_directory_name)
        create_dir_if_necessary(self.base_directory)
        self.output_directory = os.path.join(self.base_directory, RESULTS_DIRECTORY_FILENAME)
        self.workflow_path = self._add_workflow_file(cwl_file_url)
        self.job_order_file_path = self.save_data_to_directory(self.working_directory,
                                                               JOB_ORDER_FILENAME,
                                                               job_order)

    def _add_workflow_file(self, cwl_file_url):
        """
        Download a packed cwl workflow file.
        :param cwl_file_url: str: url that points to a packed cwl workflow
        :return: str: location we downloaded the to
        """
        workflow_file = os.path.join(self.working_directory, WORKFLOW_FILENAME)
        urllib.urlretrieve(cwl_file_url, workflow_file)
        return workflow_file


class CwlWorkflow(object):
    """
    Runs a CWL workflow using the cwl-runner command line program.
    1. Writes out input_json to a file
    2. Runs cwl-runner in a separate process
    3. Gathers stderr/stdout output from the process
    4. If exit status is not 0 raises JobStepFailed including output
    """
    def __init__(self, job_id, working_directory, output_directory, cwl_base_command):
        """
        Setup workflow
        :param job_id: int: job id we are running a workflow for
        :param working_directory: str: path to working directory that contains input files
        :param output_directory: str: path to output directory
        :param cwl_base_command: [str] or None: array of cwl command and arguments (osx requires special arguments)
        """
        self.job_id = job_id
        self.working_directory = working_directory
        self.output_directory = output_directory
        self.cwl_base_command = cwl_base_command

    def run(self, cwl_file_url, workflow_object_name, job_order):
        """
        Downloads the packed cwl workflow from cwl_file_url, runs it.
        If cwl-runner doesn't exit with 0 raise JobStepFailed
        :param cwl_file_url: str: url to workflow we will run (should be packed)
        :param workflow_object_name: name of the object in our workflow to execute (typically '#main')
        :param job_order: str: json string of input parameters for our workflow
        """
        cwl_directory = CwlDirectory(self.working_directory, self.output_directory, cwl_file_url, job_order)
        workflow_file = cwl_directory.workflow_path
        if workflow_object_name:
            workflow_file += workflow_object_name
        process = CwlWorkflowProcess(self.cwl_base_command,
                                     cwl_directory.output_directory,
                                     workflow_file,
                                     cwl_directory.job_order_file_path)
        process.run()
        results_directory = ResultsDirectory(self.job_id, cwl_directory)
        results_directory.add_files(process)
        if process.return_code != 0:
            error_message = "CWL workflow failed with exit code: {}".format(process.return_code)
            raise JobStepFailed(error_message + process.error_output, process.output)


class CwlWorkflowProcess(object):
    def __init__(self, cwl_base_command, local_output_directory, workflow_file, job_order_filename):
        """
        Setup to run cwl workflow using the supplied parameters.
        :param cwl_base_command:  [str] or None: array of cwl command and arguments (osx requires special arguments)
        :param local_output_directory: str: path to directory we will save output files into
        :param workflow_file: str: path to the cwl workflow
        :param job_order_filename: str: path to the cwl job order (input file)
        """
        self.output = ""
        self.error_output = ""
        self.return_code = None
        self.started = None
        self.finished = None
        base_command = cwl_base_command
        if not base_command:
            base_command = [RUN_CWL_COMMAND]
        self.command = base_command[:]
        self.command.extend([RUN_CWL_OUTDIR_ARG, local_output_directory, workflow_file, job_order_filename])

    def run(self):
        """
        Run job saving results in process_output, process_error_output, and return_code members.
        :param command: [str]: array of strings representing a workflow command and its arguments
        """
        self.started = datetime.datetime.now()
        p = Popen(self.command, stdin=PIPE, stderr=PIPE, stdout=PIPE, bufsize=1)
        self.output = ""
        self.error_output = ""
        while True:
            line = p.stderr.readline()
            if line:
                self.error_output += line + "\n"
            else:
                break
        while True:
            line = p.stdout.readline()
            if line:
                self.output += line + "\n"
            else:
                break
        p.wait()
        self.return_code = p.returncode
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
      upload_directory/           # this is a user specified name (this directory is uploaded in the store output stage)
        Bespin-Report.txt         # describes contents of the upload_directory
        results/
           ...output files from workflow
        logs/
            cwltool-output.json   #stdout from cwl-runner - json job results
            cwltool-output.log    #stderr from cwl-runner
        workflow/
          workflow.cwl            # cwl workflow we will run
          workflow.yml            # job order input file
    """
    def __init__(self, job_id, cwl_directory):
        """
        :param job_id: int: job id associated with this job
        :param cwl_directory: CwlDirectory: directory data for a job that has been run
        """
        self.job_id = job_id
        self.base_directory = cwl_directory.base_directory
        self.workflow_path = cwl_directory.workflow_path
        self.job_order_file_path = cwl_directory.job_order_file_path

    def add_files(self, cwl_process):
        """
        Add output files to the resulting directory based on the finished process.
        :param cwl_process: CwlProcess: process that was run - contains stdout, stderr, and exit status
        """
        self._create_log_files(cwl_process.output, cwl_process.error_output)
        self._copy_workflow_inputs()
        self._create_report(cwl_process)

    def _create_log_files(self, output, error_output):
        """
        Add stdout and stderr from the cwl-runner process to the 'logs' directory.
        :param output: str: stdout from cwl-runner
        :param error_output:  str: stderr from cwl-runner
        """
        logs_directory = os.path.join(self.base_directory, LOGS_DIRECTORY)
        create_dir_if_necessary(logs_directory)
        save_data_to_directory(logs_directory, JOB_STDOUT_FILENAME, output)
        save_data_to_directory(logs_directory, JOB_STDERR_FILENAME, error_output)

    def _copy_workflow_inputs(self):
        """
        Copies workflow input files to the 'workflow' directory.
        """
        workflow_directory = os.path.join(self.base_directory, WORKFLOW_DIRECTORY)
        create_dir_if_necessary(workflow_directory)
        shutil.copy(self.workflow_path, os.path.join(workflow_directory, WORKFLOW_FILENAME))
        shutil.copy(self.job_order_file_path, os.path.join(workflow_directory, JOB_ORDER_FILENAME))

    def _create_report(self, cwl_process):
        """
        Creates a report to the directory that will be uploaded based on the inputs and outputs of the workflow.
        :param cwl_process: CwlProcess: contains job start/stop info
        """
        logs_directory = os.path.join(self.base_directory, LOGS_DIRECTORY)
        workflow_directory = os.path.join(self.base_directory, WORKFLOW_DIRECTORY)
        workflow_info = create_workflow_info(workflow_path=os.path.join(workflow_directory, WORKFLOW_FILENAME))
        workflow_info.update_with_job_order(job_order_path=os.path.join(workflow_directory, JOB_ORDER_FILENAME))
        workflow_info.update_with_job_output(job_output_path=os.path.join(logs_directory, JOB_STDOUT_FILENAME))
        job_data = {
            'id': self.job_id,
            'started': cwl_process.started,
            'finished': cwl_process.finished,
            'run_time': cwl_process.total_runtime_str(),
            'num_output_files': workflow_info.count_output_files(),
            'total_file_size_str': workflow_info.total_file_size_str()
        }
        report = CwlReport(workflow_info, job_data)
        report.save(os.path.join(self.base_directory, REPORT_FILENAME))
