"""
Runs cwl workflow.
"""
from __future__ import print_function
import os
import urllib
from subprocess import PIPE, Popen
from lando.exceptions import JobStepFailed


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
        :param output_directory: str: path to ouput directory
        :param cwl_base_command: [str] or None: array of cwl command and arguments (osx requires special arguments)
        """
        self.job_id = job_id
        self.working_directory = working_directory
        self.output_directory = output_directory
        self.cwl_base_command = cwl_base_command

    def _write_workflow_input_file(self, input_json):
        """
        Save input_json to our output file and return path to our filename
        :param input_json: str: settings used in the workflow
        :return: str: filename we wrote the input_json into
        """
        filename = os.path.join(self.working_directory, 'workflow.yml')
        with open(filename, 'w') as outfile:
            outfile.write(input_json)
        return filename

    def _build_command(self, local_output_directory, workflow_file, workflow_input_filename):
        """
        Create an array containing "cwl-runner" and it's arguments.
        :param local_output_directory: str: path to directory we will save output files into
        :param workflow_file: str: path to the cwl workflow
        :param workflow_input_filename: str: path to the cwl workflow input file
        :return: [str]: command and arguments
        """
        base_command = self.cwl_base_command
        if not base_command:
            base_command = ["sudo", "cwl-runner"]
        command = base_command[:]
        command.extend(["--outdir", local_output_directory, workflow_file, workflow_input_filename])
        return command

    def run(self, cwl_file_url, workflow_object_name, input_json):
        """
        Downloads the packed cwl workflow from cwl_file_url, runs it.
        If cwl-runner doesn't exit with 0 raise JobStepFailed
        :param cwl_file_url: str: url to workflow we will run (should be packed)
        :param workflow_object_name: name of the object in our workflow to execute (typically '#main')
        :param input_json: str: json string of input parameters for our workflow
        """
        workflow_input_filename = self._write_workflow_input_file(input_json)
        workflow_file = os.path.join(self.working_directory, 'workflow.cwl')
        urllib.urlretrieve(cwl_file_url, workflow_file)
        if workflow_object_name:
            workflow_file += workflow_object_name
        local_output_directory = os.path.join(self.working_directory, self.output_directory)
        command = self._build_command(local_output_directory, workflow_file, workflow_input_filename)
        output, return_code = self.run_command(command)
        if return_code != 0:
            error_message = "CWL workflow failed with exit code: {}".format(return_code)
            raise JobStepFailed(error_message, output)

    @staticmethod
    def run_command(command):
        """
        Runs a command and saves the output by reading from pipes.
        :param command: [str]: command to execute
        :return: str, int: output from stderr/stdout and process exit status.
        """
        print(command)
        p = Popen(command, stdin=PIPE, stderr=PIPE, stdout=PIPE, bufsize=1)
        output = ""
        while True:
            line = p.stderr.readline()
            if line:
                output += line + "\n"
            else:
                break
        while True:
            line = p.stdout.readline()
            if line:
                output += line + "\n"
            else:
                break
        p.wait()
        return output, p.returncode