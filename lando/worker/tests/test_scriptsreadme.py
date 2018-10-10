
from unittest import TestCase
from lando.worker.scriptsreadme import ScriptsReadme


class TestCwlReport(TestCase):
    def test_render(self):
        template = '{{workflow_filename}},{{job_order_filename}}'
        report = ScriptsReadme('test.cwl', 'test.json', template)
        self.assertEqual('test.cwl,test.json', report.render_markdown())

    def test_save_converts_to_html(self):
        template = '{{workflow_filename}},{{job_order_filename}}'
        report = ScriptsReadme('test.cwl', 'test.json', template)
        self.assertEqual('<p>test.cwl,test.json</p>', report.render_html())
