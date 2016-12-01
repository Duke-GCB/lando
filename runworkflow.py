from __future__ import print_function
import yaml
import os
import urllib
from subprocess import PIPE, Popen


def make_input_file_yaml(fields):
    data = {}
    for field in fields:
        if field.type == 'dds_file':
            if field.staging == 'I':
                data[field.name] = {
                    'class': 'File',
                    'path': field.dds_file.path,
                }
            else:
                data[field.name] = field.dds_file.path
        else:
            value = field.value
            if field.type == 'integer':
                value = int(value)
            data[field.name] = value
    return yaml.safe_dump(data, default_flow_style=False)

class RunWorkflow(object):
    def __init__(self, job_id, working_directory):
        self.job_id = job_id
        self.working_directory = working_directory

    def run(self, fields):
        self.write_workflow_input_file(fields)

    def write_workflow_input_file(self, fields):
        filename = os.path.join(self.working_directory, 'workflow.yml')
        with open(filename, 'w') as outfile:
            outfile.write(make_input_file_yaml(fields))
        return filename

    def run_workflow(self, cwl_file_url, workflow_object_name, fields):
        workflow_input_filename = self.write_workflow_input_file(fields)
        workflow_file = os.path.join(self.working_directory, 'workflow.cwl')
        urllib.urlretrieve(cwl_file_url, workflow_file)
        if workflow_object_name:
            workflow_file += workflow_object_name

        command = ["cwl-runner", "--outdir", self.working_directory, workflow_file, workflow_input_filename]
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