
import jinja2
import markdown
from lando.worker.cwlreport import BaseReport

TEMPLATE = """
# Instructions on running this workflow.
- Download all input data and update {{job_order_filename}} with these new locations
- Install cwl-runner: pip install cwl-runner
- Run workflow: cwl-runner {{workflow_filename}} {{job_order_filename}}
"""


class ScriptsReadme(BaseReport):
    """
    Instructions on how to run the workflow in the scripts directory.
    """
    def __init__(self, workflow_filename, job_order_filename, template_str=TEMPLATE):
        """
        :param workflow_filename: str: name of the cwl workflow a user should run
        :param job_order_filename: str: name of the job order file a user should include
        :param template_str: str: template to use for rendering
        """
        super(ScriptsReadme, self).__init__(template_str)
        self.workflow_filename = workflow_filename
        self.job_order_filename = job_order_filename

    def render_markdown(self):
        """
        Make the report
        :return: str: report contents
        """
        return self.template.render(
            workflow_filename=self.workflow_filename,
            job_order_filename=self.job_order_filename)
