"""
Runs cwl workflow.
"""
from __future__ import print_function
import os
import shutil
import urllib
from subprocess import PIPE, Popen
from lando.exceptions import JobStepFailed
from lando.worker.cwlreport import create_workflow_info, CwlReport


JOB_STDOUT_FILENAME = 'cwltool-output.json'
JOB_STDERR_FILENAME = 'cwltool-output.log'
WORKFLOW_FILENAME = 'workflow.cwl'
JOB_ORDER_FILENAME = 'workflow.yml'
RESULTS_DIRECTORY_FILENAME = 'results'
WORKFLOW_DIRECTORY = 'workflow'
LOGS_DIRECTORY = 'logs'


class ResultDirectory(object):
    """
    Creates a directory to hold the output files to be delivered to the user.
    """
    def __init__(self, working_directory, user_directory_name, cwl_file_url, job_order):
        self.working_directory = working_directory
        self.base_directory = os.path.join(working_directory, user_directory_name)
        os.mkdir(self.base_directory)
        self.output_directory = os.path.join(self.base_directory, RESULTS_DIRECTORY_FILENAME)
        self.workflow_path = self._add_workflow_file(cwl_file_url)
        self.job_order_file_path = self._add_job_order_file(job_order)

    def _add_workflow_file(self, cwl_file_url):
        workflow_file = os.path.join(self.working_directory, WORKFLOW_FILENAME)
        urllib.urlretrieve(cwl_file_url, workflow_file)
        return workflow_file

    def _add_job_order_file(self, input_json):
        return self._save_to_directory(self.working_directory, JOB_ORDER_FILENAME, input_json)

    def create_log_files(self, output, error_output):
        logs_directory = os.path.join(self.base_directory, LOGS_DIRECTORY)
        os.mkdir(logs_directory)
        self._save_to_directory(logs_directory, JOB_STDOUT_FILENAME, output)
        self._save_to_directory(logs_directory, JOB_STDERR_FILENAME, error_output)

    def copy_workflow_inputs(self):
        workflow_directory = os.path.join(self.base_directory, WORKFLOW_DIRECTORY)
        os.mkdir(workflow_directory)
        shutil.copy(self.workflow_path, os.path.join(workflow_directory, WORKFLOW_FILENAME))
        shutil.copy(self.job_order_file_path, os.path.join(workflow_directory, JOB_ORDER_FILENAME))

    def create_report(self):
        logs_directory = os.path.join(self.base_directory, LOGS_DIRECTORY)
        workflow_directory = os.path.join(self.base_directory, WORKFLOW_DIRECTORY)

        workflow_info = create_workflow_info(workflow_path=os.path.join(workflow_directory, WORKFLOW_FILENAME))
        workflow_info.update_with_job_order(job_order_path=os.path.join(workflow_directory, JOB_ORDER_FILENAME))
        workflow_info.update_with_job_output(job_output_path=os.path.join(logs_directory, JOB_STDOUT_FILENAME))
        job_data = {
            'id': 1,
            'name': 'Bradley Lab Pig/Eagle RNA sequencing',
            'started': "2017-03-08T21:36:58.491777Z",
            'run_time': '12 hours',
            'num_output_files': workflow_info.count_output_files(),
            'total_file_size_str': workflow_info.total_file_size_str()
        }
        report = CwlReport(workflow_info, job_data)
        report.save(os.path.join(self.base_directory, 'Bespin-Report.txt'))

    @staticmethod
    def _save_to_directory(directory_path, filename, data):
        file_path = os.path.join(directory_path, filename)
        with open(file_path, 'w') as outfile:
            outfile.write(data)
        return file_path


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

    def _write_job_order_file(self, input_json):
        """
        Save input_json to our output file and return path to our filename
        :param input_json: str: settings used in the workflow
        :return: str: filename we wrote the input_json into
        """
        filename = os.path.join(self.working_directory, 'workflow.yml')
        with open(filename, 'w') as outfile:
            outfile.write(input_json)
        return filename

    def _build_command(self, local_output_directory, workflow_file, job_order_filename):
        """
        Create an array containing "cwl-runner" and it's arguments.
        :param local_output_directory: str: path to directory we will save output files into
        :param workflow_file: str: path to the cwl workflow
        :param job_order_filename: str: path to the cwl job order (input file)
        :return: [str]: command and arguments
        """
        base_command = self.cwl_base_command
        if not base_command:
            base_command = ["cwl-runner"]
        command = base_command[:]
        command.extend(["--outdir", local_output_directory, workflow_file, job_order_filename])
        return command

    def run(self, cwl_file_url, workflow_object_name, job_order):
        """
        Downloads the packed cwl workflow from cwl_file_url, runs it.
        If cwl-runner doesn't exit with 0 raise JobStepFailed
        :param cwl_file_url: str: url to workflow we will run (should be packed)
        :param workflow_object_name: name of the object in our workflow to execute (typically '#main')
        :param job_order: str: json string of input parameters for our workflow
        """
        result_directory = ResultDirectory(self.working_directory, self.output_directory, cwl_file_url, job_order)
        workflow_file = result_directory.workflow_path
        if workflow_object_name:
            workflow_file += workflow_object_name
        command = self._build_command(
            result_directory.output_directory,
            workflow_file,
            result_directory.job_order_file_path)
        output, error_output, return_code = self.run_command(command)
        result_directory.create_log_files(output, error_output)
        result_directory.copy_workflow_inputs()
        result_directory.create_report()
        if return_code != 0:
            error_message = "CWL workflow failed with exit code: {}".format(return_code)
            raise JobStepFailed(error_message + error_output, output)

    @staticmethod
    def run_command(command):
        """
        Runs a command and saves the output by reading from pipes.
        :param command: [str]: command to execute
        :return: str, int: output from stderr/stdout and process exit status.
        """
        print(' '.join(command))
        p = Popen(command, stdin=PIPE, stderr=PIPE, stdout=PIPE, bufsize=1)
        output = ""
        error_output = ""
        while True:
            line = p.stderr.readline()
            if line:
                error_output += line + "\n"
            else:
                break
        while True:
            line = p.stdout.readline()
            if line:
                output += line + "\n"
            else:
                break
        p.wait()
        return output, error_output, p.returncode
