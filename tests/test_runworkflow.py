# from unittest import TestCase
# from runworkflow import make_input_file_yaml
# from jobapi import JobField
#
# SAMPLE_PARAM_DATA = [
#     {
#         "id": 1,
#         "job": "http://127.0.0.1:8000/api/jobs/1/",
#         "name": "input_fastq_file",
#         "type": "dds_file",
#         "value": "",
#         "staging": "I",
#         "dds_file": {
#             "id": 1,
#             "project_id": "bb02ee1f-707c-420f-a11d-440d921f093b",
#             "file_id": "92bd8723-08b6-46f1-ac2e-1d9728ec907b",
#             "path": "ERR550644_1.fastq",
#             "dds_app_credentials": 1,
#             "dds_user_credentials": 1
#         }
#     },
#     {
#         "id": 2,
#         "job": "http://127.0.0.1:8000/api/jobs/1/",
#         "name": "output_qc_report_filename",
#         "type": "dds_file",
#         "value": "",
#         "staging": "O",
#         "dds_file": {
#             "id": 2,
#             "project_id": "bb02ee1f-707c-420f-a11d-440d921f093b",
#             "file_id": None,
#             "path": "results.zip",
#             "dds_app_credentials": 1,
#             "dds_user_credentials": 1
#         }
#     },
#     {
#         "id": 3,
#         "job": "http://127.0.0.1:8000/api/jobs/1/",
#         "name": "threads",
#         "type": "integer",
#         "value": "4",
#         "staging": "P",
#         "dds_file": None
#     }
# ]
#
#
# class TestRunWorkflow(TestCase):
#     def test_make_input_file_yaml(self):
#         fields = [JobField(field_data) for field_data in SAMPLE_PARAM_DATA]
#         working_directory = 'data'
#         expected = """
# input_fastq_file:
#   class: File
#   path: ERR550644_1.fastq
# output_qc_report_filename: data/results.zip
# threads: 4
# """
#         input_file_yaml = make_input_file_yaml(working_directory, fields)
#         self.assertMultiLineEqual(expected.strip(), input_file_yaml.strip())