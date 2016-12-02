from __future__ import print_function
import yaml
import os
import urllib
from subprocess import PIPE, Popen


class RunWorkflow(object):
    def __init__(self, job_id, working_directory, output_directory):
        self.job_id = job_id
        self.working_directory = working_directory
        self.output_directory = output_directory

    def run(self, fields):
        self.write_workflow_input_file(fields)

    def write_workflow_input_file(self, input_json):
        filename = os.path.join(self.working_directory, 'workflow.yml')
        with open(filename, 'w') as outfile:
            outfile.write(input_json)
        return filename

    def run_workflow(self, cwl_file_url, workflow_object_name, input_json):
        workflow_input_filename = self.write_workflow_input_file(input_json)
        workflow_file = os.path.join(self.working_directory, 'workflow.cwl')
        urllib.urlretrieve(cwl_file_url, workflow_file)
        if workflow_object_name:
            workflow_file += workflow_object_name

        local_output_directory = os.path.join(self.working_directory, self.output_directory)

        #command = ["sudo", "cwl-runner", "--outdir", self.working_directory, workflow_file, workflow_input_filename]
        command = ["cwl-runner", "--outdir", local_output_directory,
                   workflow_file, workflow_input_filename]
        print(command)
        p = Popen(command, stdin=PIPE, stderr=PIPE, stdout=PIPE, bufsize=1)
        while True:
            line = p.stderr.readline()
            if line:
                self.cwl_output_func(line)
            else:
                break
        while True:
            line = p.stdout.readline()
            if line:
                self.cwl_output_func(line)
            else:
                break

    def cwl_output_func(self, line):
        print("got", line)