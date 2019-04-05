# -*- coding: utf-8 -*-


from unittest import TestCase
from lando.worker.cwlreport import CwlReport, get_documentation_str, create_workflow_info, upconvert_to_list
from unittest.mock import patch, MagicMock, Mock

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
        },
        {
            "type": "Directory",
            "outputSource": "#main/organize_directories/bams_markduplicates_dir",
            "doc": "BAM and bai files from markduplicates",
            "id": "#main/bams_markduplicates_dir"
        },
    ],
}

SAMPLE_CWL_UNPACKED_WORKFLOW = {
    'cwlVersion': 'v1.0',
    'class': 'Workflow',
    'inputs': {
        'input1': 'File',
        'input2': 'int'
    },
    'outputs': {
        'output1': 'File'
    }
}

SAMPLE_JOB_ORDER = {
    "gff_file": {
      "class": "File",
      "path": "ant.gff"
    },
    "threads": 20
}

# Need an example workflow here that has a paired input.
SAMPLE_WORKFLOW_PAIR_INPUT= {
    'cwlVersion': 'v1.0',
    "id": "#main",
    "doc": "A really good workflow",
    "inputs": [
        {
            "type": "#bespin-types.yml/NamedFASTQFilePairType",
            "id": "#extract-named-file-pair-details.cwl/read_pair"
        }
    ],
    "outputs": []
}

SAMPLE_JOB_ORDER_PAIR_INPUT = {
    "read_pair": {
        "file1": {
            "class": "File",
            "path": "read1.fastq"
        },
        "file2": {
            "class": "File",
            "path": "read2.fastq"
        },
        "name": "read"
    }
}

SAMPLE_JOB_OUTPUT = {
    "align_log": [
        {
            "class": "File",
            "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a814",
            "location": "/tmp/align_log.txt",
            "size": 900,
            "secondaryFiles": [
                {
                    "class": "File",
                    "checksum": "sha1$6ec2f899946f8091693ce65cc6323958695dec21",
                    "location": "/tmp/align_log.idx",
                    "size": 123
                }
            ]
        }
    ],
    "aligned_read": {
        "class": "File",
        "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a814",
        "location": "/tmp/aligned_read.txt",
        "size": 1010123,
        "secondaryFiles": [
            {
                "class": "File",
                "checksum": "sha1$6ec2f899946f8091693ce65cc6323958695dec20",
                "location": "/tmp/aligned_read.idx",
                "size": 6613916
            }
        ]
    },
    "trim_reports": [
        [
            {
                "class": "File",
                "checksum": "sha1$0e724dda4c96d901af1ecd53d0cd5882d6b1a815",
                "location": "/tmp/trim_report.txt",
                "size": 44,
            }
        ]
    ],
    "bams_markduplicates_dir": {
        "class": "Directory",
        "basename": "bams-markduplicates",
        "location": "file:///work/data_for_job_49/working/results/bams-markduplicates",
        "listing": [
            {
                "checksum": "sha1$878d31004d7867b9ded64426657519a28b7ce46b",
                "location": "file:///work/data_for_job_49/working/results/bams-markduplicates/SA03505-dedup.bam",
                "secondaryFiles": [
                    {
                        "checksum": "sha1$2144e6c9778fa3564e4b1a4069cee3a7f3ef9d1b",
                        "location": "file:///work/data_for_job_49/working/results/bams-markduplicates/SA03505-dedup.bai",
                        "class": "File",
                        "size": 6218232
                    }
                ],
                "class": "File",
                "size": 5570967966
            },
            {
                "checksum": "sha1$59697e90052dc258b6887e8c84eb42c2f5ae0842",
                "location": "file:///work/data_for_job_49/working/results/bams-markduplicates/SA03567-dedup.bam",
                "secondaryFiles": [
                    {
                        "checksum": "sha1$9486797d5dd4c2c757b853bc21d64f790988b207",
                        "location": "file:///work/data_for_job_49/working/results/bams-markduplicates/SA03567-dedup.bai",
                        "class": "File",
                        "size": 6309968
                    }
                ],
                "class": "File",
                "size": 7253903222
            },
        ]
    }
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
        self.assertEqual('test 123', report.render_markdown())

    def test_save_converts_to_html(self):
        template = '{{workflow.data}} {{job.job_id}}'
        workflow_info = MagicMock(data='test')
        job_data = MagicMock(job_id=123)
        report = CwlReport(workflow_info, job_data, template)
        report.render_html()
        self.assertEqual("<p>test 123</p>", report.render_html())

    def test_handles_unicode(self):
        template = '{{workflow.data}}qüü{{job.job_id}}'
        workflow_info = MagicMock(data='ää')
        job_data = MagicMock(job_id='üä')
        report = CwlReport(workflow_info, job_data, template)
        rendered = report.render_html()
        self.assertEqual('<p>ääqüüüä</p>', rendered)


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
        self.assertEqual("Unable to read /tmp/fakepath.cwl as CWL", str(err.exception))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_with_top_level_graph(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = {
            "$graph": [
                SAMPLE_CWL_MAIN_DATA
            ]
        }
        workflow = create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual(2, len(workflow.input_params))
        self.assertEqual(4, len(workflow.output_data))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_with_no_graph(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = SAMPLE_CWL_MAIN_DATA
        workflow = create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual(2, len(workflow.input_params))
        self.assertEqual(4, len(workflow.output_data))

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_create_workflow_info_from_unpacked(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = SAMPLE_CWL_UNPACKED_WORKFLOW
        workflow = create_workflow_info('/tmp/fakepath.cwl')
        self.assertEqual(2, len(workflow.input_params))
        self.assertEqual(1, len(workflow.output_data))


class UpconvertToListTestCase(TestCase):

    def test_upconverts_dict(self):
        inputs = {'input1': 'File'}
        converted = upconvert_to_list(inputs)
        self.assertEqual(converted, [{'id': 'input1', 'type': 'File'}])

    def test_returns_list_unmodified(self):
        inputs = [Mock(), Mock()]
        converted = upconvert_to_list(inputs)
        self.assertEqual(converted, inputs)

    def test_raises_on_other(self):
        inputs = {Mock}
        with self.assertRaises(ValueError) as context:
            upconvert_to_list(inputs)
        self.assertIn('Only list or dict are supported', str(context.exception))


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

        self.assertEqual(4, len(workflow.output_data))
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
        output_data = workflow.output_data[3]
        self.assertEqual('bams_markduplicates_dir', output_data.name)
        self.assertEqual(4, len(output_data.files))
        self.assertEqual("SA03505-dedup.bam", output_data.files[0].filename)
        self.assertEqual(5570967966, output_data.files[0].size)
        self.assertEqual("SA03505-dedup.bai", output_data.files[1].filename)
        self.assertEqual("SA03567-dedup.bam", output_data.files[2].filename)
        self.assertEqual("SA03567-dedup.bai", output_data.files[3].filename)
        self.assertEqual('12.85 GB', workflow.total_file_size_str())

    @patch("lando.worker.cwlreport.parse_yaml_or_json")
    def test_extracts_path_in_pair_input_object(self, mock_parse_yaml_or_json):
        mock_parse_yaml_or_json.return_value = SAMPLE_WORKFLOW_PAIR_INPUT
        workflow = create_workflow_info('/tmp/fake_packed_workflow.cwl')
        mock_parse_yaml_or_json.return_value = SAMPLE_JOB_ORDER_PAIR_INPUT
        workflow.update_with_job_order('/tmp/fake_job_order.json')
        self.assertEqual(len(workflow.input_params), 1)
        self.assertEqual(1, len(workflow.input_params))

        input_param = workflow.input_params[0]
        self.assertEqual('read_pair', input_param.name)
        self.assertEqual('read1.fastq,read2.fastq', input_param.value)
