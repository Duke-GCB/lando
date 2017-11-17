"""
Creates a report about the inputs and outputs of a cwl workflow.
"""
from __future__ import print_function
import os
import sys
import yaml
import jinja2
import humanfriendly
import markdown
import codecs

TEMPLATE = """
# Summary

Job: {{ workflow.documentation }}
Job Id: {{ job.id }}
Started: {{ job.started }}
Finished: {{ job.finished }}
Run time: {{ job.run_time }}
Output: {{ job.num_output_files }} files ({{ job.total_file_size_str }})

{{ job.workflow_methods }}

# Input
{% for param in workflow.input_params %}
{{ param.documentation }}
{{ param.value }}

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


class BaseReport(object):
    """
    Base report class that assumes subclass will implement render_markdown() that returns markdown format
    """
    def __init__(self, template_str):
        self.template = jinja2.Template(template_str)

    def render_markdown(self):
        """
        This method should be overridden in a subclass
        :return: str: report contents
        """
        return ''

    def render_html(self):
        """
        Return README content in html format
        :return: str: report contents
        """
        return markdown.markdown(self.render_markdown())


class CwlReport(BaseReport):
    """
    Report detailing inputs and outputs of a cwl workflow that has been run.
    """
    def __init__(self, workflow_info, job_data, template_str=TEMPLATE):
        """
        :param workflow_info: WorkflowInfo: info derived from cwl input/output files
        :param job_data: dict: data used in report from non-cwl sources
        :param template_str: str: template to use for rendering
        """
        super(CwlReport, self).__init__(template_str)
        self.workflow_info = workflow_info
        self.job_data = job_data

    def render_markdown(self):
        """
        Return README content in markdown format
        :return: str: report contents
        """
        return self.template.render(workflow=self.workflow_info, job=self.job_data)


def get_documentation_str(node):
    """
    Retrieve the documentation information from a cwl formatted dictionary.
    If there is no doc tag return the id value.
    :param node: dict: cwl dictionary
    :return: str: documentation description or id
    """
    documentation = node.get("doc")
    if not documentation:
        documentation = node.get("id")
    return documentation


def create_workflow_info(workflow_path):
    """
    Create a workflow_info filling in data based on a packed cwl workflow.
    :param workflow_path: str: packed cwl workflow
    :return: WorkflowInfo
    """
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


class WorkflowInfo(object):
    """
    Information about the workflow derived from cwl input/output files.
    """
    def __init__(self, workflow_filename, cwl_version, workflow_node):
        """
        :param workflow_filename: str: path to a packed cwl workflow
        :param cwl_version: str: version of cwl used in workflow_filename
        :param workflow_node: dict: cwl dictionary inside workflow_filename
        """
        self.workflow_filename = workflow_filename
        self.job_order_filename = None
        self.job_output_filename = None
        self.cwl_version = cwl_version
        self.documentation = get_documentation_str(workflow_node)
        self.input_params = []
        self.output_data = []
        for input_param in workflow_node.get("inputs"):
            self.input_params.append(InputParam(input_param))
        for output_param in workflow_node.get("outputs"):
            self.output_data.append(OutputData(output_param))

    def update_with_job_order(self, job_order_path):
        """
        Updates internal state based on job order data
        :param job_order_path: str: path to the job_order file used with workflow_name
        """
        self.job_order_filename = job_order_path
        doc = parse_yaml_or_json(job_order_path)
        for key in doc.keys():
            val = doc.get(key)
            input_param = find_by_name(key, self.input_params)
            if input_param:
                input_param.set_value(self._create_str_value(val))

    @staticmethod
    def _create_str_value(val):
        """
        Coerce cwl value into str.
        """
        if type(val) == dict:
            if 'path' in val:
                return val['path']
            else:
                # may be a custom object containing files, extract
                parts = [val[part] for part in val if 'path' in val[part]]
                return WorkflowInfo._create_str_value(parts)
        if type(val) == list:
            return ','.join(sorted([WorkflowInfo._create_str_value(part) for part in val]))
        return str(val)

    def update_with_job_output(self, job_output_path):
        """
        Updates internal state based on stdout (resulting json) from running cwl
        :param job_output_path: str: path to resulting json object from running cwl
        """
        self.job_output_filename = job_output_path
        doc = parse_yaml_or_json(job_output_path)
        if doc:
            for key in doc.keys():
                val = doc.get(key)
                out_data = find_by_name(key, self.output_data)
                self._add_files_recursive(out_data, val)

    def _add_files_recursive(self, out_data, node):
        """
        Recursively adds files contained under node to out_data.
        :param out_data: OutputData: contains all files for a workflow output name.
        :param node: dict/array: either file location information or array(possibly nested)
        """
        if type(node) == dict:
            if node.get('class') == 'File':
                out_data.add_file(OutputFile(node))
                secondaryFiles = node.get("secondaryFiles")
                if secondaryFiles:
                    self._add_files_recursive(out_data, secondaryFiles)
            elif node.get('class') == 'Directory':
                for listing_item in node.get("listing", []):
                    self._add_files_recursive(out_data, listing_item)
        else:
            for item in node:
                self._add_files_recursive(out_data, item)

    def count_output_files(self):
        return len(self.output_data)

    def total_file_size_str(self):
        """
        Returns the total output file size as a human friendly string.
        """
        number_bytes = 0
        for item in self.output_data:
            for file_data in item.files:
                number_bytes += file_data.size
        return humanfriendly.format_size(number_bytes)


def find_by_name(name, items):
    """
    Find the object with the specified name attribute.
    :param name: str: name to look for
    :param items: [object]: list of objects to look for
    :return: first item matching name or None if not found
    """
    for item in items:
        if item.name == name:
            return item
    return None


class InputParam(object):
    """
    Input parameter for a cwl workflow.
    """
    def __init__(self, data):
        self.name = os.path.basename(data.get('id'))
        self.documentation = get_documentation_str(data)
        self.value = None

    def set_value(self, value):
        """
        Saves str_value and raises error if called more than once.
        :param value: str: user facing value
        """
        if self.value:
            raise ("Duplicate value for {} : {}".format(self.name, value))
        self.value = value


class OutputData(object):
    """
    Output file generated by a cwl workflow.
    """
    def __init__(self, data):
        """
        :param data: dict: cwl formatted output data dictionary
        """
        self.name = os.path.basename(data.get('id'))
        self.documentation = get_documentation_str(data)
        self.files = []

    def add_file(self, output_file):
        """
        Add an output file to the list associated with this output data.
        :param output_file: OutputFile: info about the file
        """
        self.files.append(output_file)


class OutputFile(object):
    """
    Single file created by a CWL workflow
    """
    def __init__(self, data):
        """
        :param data: dict: cwl formatted file info
        """
        self.filename = os.path.basename(data.get('location'))
        self.checksum = data.get('checksum')
        self.size = data.get('size', 0)


def parse_yaml_or_json(path):
    """
    Return parsed YAML or JSON for a path to a file.
    """
    with codecs.open(path, mode='r', encoding='utf-8') as infile:
        doc = yaml.load(infile)
    return doc


def main():
    """
    Method to allow easy testing of report contents.
    """
    if len(sys.argv) != 5:
        print("usage: python lando/worker/cwlreport.py <CWL_WORKFLOW_FILENAME> <CWL_JOB_ORDER_FILENAME> "
              "<CWLTOOL_OUTPUT_FILENAME> <JOB_DATA_FILENAME>")
        sys.exit(1)
    else:
        workflow_info = create_workflow_info(workflow_path=sys.argv[1])
        workflow_info.update_with_job_order(job_order_path=sys.argv[2])
        workflow_info.update_with_job_output(job_output_path=sys.argv[3])
        job_data = parse_yaml_or_json(path=sys.argv[4])
        report = CwlReport(workflow_info, job_data)
        print(report.render_markdown())


if __name__ == "__main__":
    main()
