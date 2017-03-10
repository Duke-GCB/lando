from __future__ import print_function
import os
import yaml
import jinja2


TEMPLATE = """
# Summary

Job: {{ workflow.documentation }}
Job Id: {{ job.id }}
Started: {{ job.started }}
Finished: {{ job.finished }}
Run time: {{ job.run_time }}
Output: {{ job.num_output_files }} files ({{ job.total_file_size_str }})

# Input
{% for param in workflow.input_params %}
{{ param.documentation }}
{{ param.str_value }}

{% endfor %}

# Results
{% for item in workflow.output_data %}
{{ item.documentation }}
  {% for file in item.files %}
{{ file.filename }}  checksum:{{ file.checksum}}  size:{{ file.size }}
  {% endfor %}
{% endfor %}

# Reproducing

Retrieve all data and update {{ workflow.job_order_filename }} settings for where you put them.
Install cwltool
Run this command:
cwtool {{ workflow.workflow_filename }} {{ workflow.job_order_filename }}

You can compare the output from this tool against {{ workflow.job_output_filename }}.
"""


class CwlReport(object):
    def __init__(self, workflow_info, job_data):
        self.workflow_info = workflow_info
        self.job_data = job_data

    def render(self):
        template = jinja2.Template(TEMPLATE)
        return template.render(workflow=self.workflow_info, job=self.job_data)

    def save(self, destination_path):
        with open(destination_path, 'w') as outfile:
            outfile.write(self.render())


def get_documentation_str(node):
    documentation = node.get("doc")
    if not documentation:
        documentation = node.get("id")
    return documentation

class WorkflowInfo(object):
    def __init__(self, workflow_filename, cwl_version, workflow_node):
        self.workflow_filename = workflow_filename
        self.job_order_filename = None
        self.job_output_filename = None
        self.cwl_version = cwl_version
        self.documentation = get_documentation_str(workflow_node)
        self.input_params = []
        self.output_data = []
        for input_param in workflow_node.get("inputs"):
            self.add_input_param(InputParam(input_param))
        for output_param in workflow_node.get("outputs"):
            self.add_output_data(OutputData(output_param))

    def add_input_param(self, input_param):
        self.input_params.append(input_param)

    def add_output_data(self, out_data):
        self.output_data.append(out_data)

    def update_with_job_order(self, job_order_path):
        self.job_order_filename = job_order_path
        doc = parse_yaml_or_json(job_order_path)
        for key in doc.keys():
            val = doc.get(key)
            input_param = find_by_name(key, self.input_params)
            if input_param:
                input_param.set_value(val, self.create_str_value(val))

    @staticmethod
    def create_str_value(val):
        if type(val) == dict:
            return val['path']
        if type(val) == list:
            return ','.join([WorkflowInfo.create_str_value(part) for part in val])
        return str(val)

    def update_with_job_output(self, job_output_path):
        self.job_output_filename = job_output_path
        doc = parse_yaml_or_json(job_output_path)
        if doc:
            for key in doc.keys():
                val = doc.get(key)
                out_data = find_by_name(key, self.output_data)
                if type(val) == dict:
                    out_data.add_file(OutputFile(val))
                else:
                    for item in val:
                        out_data.add_file(OutputFile(item))

    def count_output_files(self):
        return len(self.output_data)

    def total_file_size_str(self):
        return '200? GB'


def find_by_name(name, items):
    for item in items:
        if item.name == name:
            return item
    return None


class InputParam(object):
    def __init__(self, data):
        self.name = os.path.basename(data.get('id'))
        self.documentation = get_documentation_str(data)
        self.value = []
        self.str_value = ''

    def set_value(self, value, str_value):
        if self.value:
            raise ("Duplicate value for {} : {}".format(self.name, value))
        self.value = value
        self.str_value = str_value


class OutputData(object):
    def __init__(self, data):
        self.name = os.path.basename(data.get('id'))
        self.documentation = get_documentation_str(data)
        self.files = []

    def add_file(self, output_file):
        self.files.append(output_file)


class OutputFile(object):
    def __init__(self, data):
        self.filename = os.path.basename(data.get('location'))
        self.checksum = data.get('checksum')
        self.size = data.get('size')


def parse_yaml_or_json(path):
    with open(path) as infile:
        doc = yaml.load(infile)
    return doc


def create_workflow_info(workflow_path):
    doc = parse_yaml_or_json(workflow_path)
    cwl_version = doc.get('cwlVersion')
    graph = doc.get("$graph")
    if graph:
        for node in graph:
            if node.get("id") == "#main":
                return WorkflowInfo(workflow_path, cwl_version, node)
    if doc.get("id") == "#main":
        return WorkflowInfo(workflow_path, cwl_version, doc)
    raise ValueError("Unable to find #main in {}".format(workflow_path))


# def main():
#     workflow_info = create_workflow_info(workflow_path="../../somedata/workflow/workflow.cwl")
#     workflow_info.update_with_job_order(job_order_path="../../somedata/workflow/workflow.yml")
#     workflow_info.update_with_job_output(job_output_path="../../somedata/logs/cwltool-output.json")
#     job_data = {
#         'id': 1,
#         'started': "2017-03-08T21:36:58.491777Z",
#         'finished': "2017-03-08T21:36:58.491777Z",
#         'run_time': '12 hours',
#         'num_output_files': workflow_info.count_output_files(),
#         'total_file_size_str': workflow_info.total_file_size_str()
#     }
#     report = CwlReport(workflow_info, job_data)
#     print(report.render())


if __name__ == "__main__":
    main()
