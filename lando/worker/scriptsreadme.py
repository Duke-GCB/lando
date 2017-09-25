from __future__ import print_function
import jinja2
import markdown

TEMPLATE = """
# Instructions on running this workflow.
- Download all input data and update {{job_order_filename}} with these new locations
- Install cwl-runner: pip install cwl-runner
- Run workflow: cwl-runner {{workflow_filename}} {{job_order_filename}}
"""


class ScriptsReadme(object):
    """
    Instructions on how to run the workflow in the scripts directory.
    """
    def __init__(self, workflow_filename, job_order_filename, template=TEMPLATE):
        self.workflow_filename = workflow_filename
        self.job_order_filename = job_order_filename
        self.template = template

    def render(self):
        """
        Make the report
        :return: str: report contents
        """
        template = jinja2.Template(self.template)
        return template.render(workflow_filename=self.workflow_filename, job_order_filename=self.job_order_filename)

    def save(self, destination_path):
        """
        Save the report to destination_path
        :param destination_path: str: path to where we will write the report
        """
        with open(destination_path, 'w') as outfile:
            outfile.write(markdown.markdown(self.render()).encode('utf8'))
