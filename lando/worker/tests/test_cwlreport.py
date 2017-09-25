from __future__ import absolute_import
from unittest import TestCase
from lando.worker.cwlreport import CwlReport, get_documentation_str, create_workflow_info
from mock import patch, MagicMock, mock_open, call

SAMPLE_CWL_MAIN_DATA = {
    'cwlVersion': 'v1.0',
    "id": "#main",
    "doc": "A really good workflow",
    "inputs": [
        {
            "doc": "The GFF File",
            "type": "File",
            "id": "#main/gff_file",
        },
        {
            "type": "int",
            "id": "#main/threads",
        }
    ],
    "outputs": [
        {
            "doc": "The Align Log",
            "outputSource": "#main/align/log",
            "type": "File",
            "id": "#main/align_log"
        },
        {
            "outputSource": "#main/align/aligned",
            "type": "File",
            "id": "#main/aligned_read"
        },
        {
            "outputSource": "#main/trim/trim_report",
            "type": "File",
            "id": "#main/trim_reports"
        }
    ],
}

SAMPLE_JOB_ORDER = {
    "gff_file": {
      "class": "File",
      "path": "ant.gff"
    },
    "threads": 20
}

SAMPLE_JOB_OUTPUT = {
    "align_log": [
        {
            "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a814",
            "location": "/tmp/align_log.txt",
            "size": 900,
            "secondaryFiles": [
                {
                    "checksum": "sha1$6ec2f899946f8091693ce65cc6323958695dec21",
                    "location": "/tmp/align_log.idx",
                    "size": 123
                }
            ]
        }
    ],
    "aligned_read": {
        "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a814",
        "location": "/tmp/aligned_read.txt",
        "size": 1010123,
        "secondaryFiles": [
            {
                "checksum": "sha1$6ec2f899946f8091693ce65cc6323958695dec20",
                "location": "/tmp/aligned_read.idx",
                "size": 6613916
            }
        ]
    },
    "trim_reports": [
        [
            {
                "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a815",
                "location": "/tmp/trim_report.txt",
                "size": 44,
            }
        ]
    ]
}


class TestCwlReport(TestCase):
    def test_render(self):
        """
        The report renders workflow and job data into a template
        """
        template = '{{workflow.data}} {{job.job_id}}'
        workflow_info = MagicMock(data='test')
        job_data = MagicMock(job_id=123)
        report = CwlReport(workflow_info, job_data, template)
        self.assertEqual('test 123', report.render())

    def test_save_converts_to_html(self):
        template = '{{workflow.data}} {{job.job_id}}'
        workflow_info = MagicMock(data='test')
        job_data = MagicMock(job_id=123)
        report = CwlReport(workflow_info, job_data, template)
        mocked_open = mock_open(read_data='file contents\nas needed\n')
        with patch('lando.worker.cwlreport.open', mocked_open):
            report.save('/tmp/fakedir/fakefile')
        self.assertEqual(call("<p>test 123</p>"), mocked_open.return_value.write.call_args)

class TestCwlReportUtilities(TestCase):
    def test_get_documentation_str(self):
        """
        get_documentation_str should return "doc", else "id", else None.
        """
        self.assertEqual("Number threads", get_documentation_str({"doc": "Number threads", "id": "123"}))
        self.assertEqual("123", get_documentation_str({"id": "123"}))
        self.assertEqual(None, get_documentation_str({}))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_bad_data(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = {}
        with self.assertRaises(ValueError) as err:
            create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual("Unable to find #main in /tmp/fakepath.cwl", str(err.exception))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_with_top_level_graph(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = {
            "$graph": [
                SAMPLE_CWL_MAIN_DATA
            ]
        }
        workflow = create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual(2, len(workflow.input_params))
        self.assertEqual(3, len(workflow.output_data))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_with_no_graph(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = SAMPLE_CWL_MAIN_DATA
        workflow = create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual(2, len(workflow.input_params))
        self.assertEqual(3, len(workflow.output_data))


class TestWorkflowInfo(TestCase):
    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_all_parts(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = SAMPLE_CWL_MAIN_DATA
        workflow = create_workflow_info('/tmp/fake_packed_workflow.cwl')
        mock_parse_yaml_or_json.return_value = SAMPLE_JOB_ORDER
        workflow.update_with_job_order('/tmp/fake_job_order.json')
        mock_parse_yaml_or_json.return_value = SAMPLE_JOB_OUTPUT
        workflow.update_with_job_output('/tmp/fake_job_output.json')

        self.assertEqual('/tmp/fake_packed_workflow.cwl', workflow.workflow_filename)
        self.assertEqual('/tmp/fake_job_order.json', workflow.job_order_filename)
        self.assertEqual('/tmp/fake_job_output.json', workflow.job_output_filename)
        self.assertEqual("v1.0", workflow.cwl_version)
        self.assertEqual("A really good workflow", workflow.documentation)

        self.assertEqual(2, len(workflow.input_params))
        input_param = workflow.input_params[0]
        self.assertEqual('gff_file', input_param.name)
        self.assertEqual('The GFF File', input_param.documentation)
        self.assertEqual('ant.gff', input_param.value)
        input_param = workflow.input_params[1]
        self.assertEqual('threads', input_param.name)
        self.assertEqual('#main/threads', input_param.documentation)
        self.assertEqual('20', input_param.value)

        self.assertEqual(3, len(workflow.output_data))
        output_data = workflow.output_data[0]
        self.assertEqual('The Align Log', output_data.documentation)
        self.assertEqual(2, len(output_data.files))
        self.assertEqual("align_log.txt", output_data.files[0].filename)
        self.assertEqual("align_log.idx", output_data.files[1].filename)
        output_data = workflow.output_data[1]
        self.assertEqual('#main/aligned_read', output_data.documentation)
        self.assertEqual(2, len(output_data.files))
        self.assertEqual("aligned_read.txt", output_data.files[0].filename)
        self.assertEqual("aligned_read.idx", output_data.files[1].filename)
        output_data = workflow.output_data[2]
        self.assertEqual(1, len(output_data.files))
        self.assertEqual("trim_report.txt", output_data.files[0].filename)
