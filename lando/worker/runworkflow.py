from __future__ import print_function
import os
import urllib
from subprocess import PIPE, Popen
from lando.exceptions import JobStepFailed


class RunWorkflow(object):
    def __init__(self, job_id, working_directory, output_directory, cwl_base_command):
        self.job_id = job_id
        self.working_directory = working_directory
        self.output_directory = output_directory
        self.cwl_base_command = cwl_base_command

    def run(self, fields):
        self.write_workflow_input_file(fields)

    def write_workflow_input_file(self, input_json):
        filename = os.path.join(self.working_directory, 'workflow.yml')
        with open(filename, 'w') as outfile:
            outfile.write(input_json)
        return filename

    def build_command(self, local_output_directory, workflow_file, workflow_input_filename):
        base_command = self.cwl_base_command
        if not base_command:
            base_command = ["sudo", "cwl-runner"]
        command = base_command[:]
        command.extend(["--outdir", local_output_directory, workflow_file, workflow_input_filename])
        return command

    def run_workflow(self, cwl_file_url, workflow_object_name, input_json):
        workflow_input_filename = self.write_workflow_input_file(input_json)
        workflow_file = os.path.join(self.working_directory, 'workflow.cwl')
        urllib.urlretrieve(cwl_file_url, workflow_file)
        if workflow_object_name:
            workflow_file += workflow_object_name
        local_output_directory = os.path.join(self.working_directory, self.output_directory)
        command = self.build_command(local_output_directory, workflow_file, workflow_input_filename)
        output, return_code = self.run_command(command)
        if return_code != 0:
            error_message = "CWL workflow failed with exit code: {}".format(return_code)
            raise JobStepFailed(error_message, output)

    @staticmethod
    def run_command(command):
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