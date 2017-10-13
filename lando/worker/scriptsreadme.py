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

    def render_markdown(self):
        """
        Make the report
        :return: str: report contents
        """
        template = jinja2.Template(self.template)
        return template.render(workflow_filename=self.workflow_filename, job_order_filename=self.job_order_filename)

    def render_html(self):
        """
        Return README content in html format
        :return: str: report contents
        """
        return markdown.markdown(self.render_markdown()).encode('utf8')
