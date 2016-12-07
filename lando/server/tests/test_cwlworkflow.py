from __future__ import absolute_import
from unittest import TestCase
import os
import platform
import tempfile
import shutil
from lando.worker.cwlworkflow import CwlWorkflow
from lando.exceptions import JobStepFailed

SAMPLE_WORKFLOW = """
cwlVersion: v1.0
class: CommandLineTool
baseCommand: cat
stdout: $(inputs.outputfile)
inputs:
 files:
    type:
      type: array
      items: File
    inputBinding:
      position: 1
outputs:
  outputfile:
    type: stdout
"""


def text_to_file(text, file_path):
    with open(file_path, 'w') as outfile:
        outfile.write(text)


def file_to_text(file_path):
    with open(file_path, 'r') as infile:
        return infile.read()


class TestCwlWorkflow(TestCase):
    def make_input_files(self, input_file_directory):
        one_path = os.path.join(input_file_directory, 'one.txt')
        text_to_file("one\n", one_path)
        two_path = os.path.join(input_file_directory, 'two.txt')
        text_to_file("two\n", two_path)
        return one_path, two_path

    def test_simple_workflow_succeeds(self):
        """
        Tests a simple cwl workflow to make sure CwlWorkflow connects all inputs/outputs correctly.
        """
        job_id = 1
        working_directory = tempfile.mkdtemp()
        output_directory = 'result'
        cwl_base_command = None
        if platform.system() == 'Darwin':
            cwl_base_command = [
                "cwl-runner",
                "--debug",
                "--tmpdir-prefix=/Users/jpb67",
                "--tmp-outdir-prefix=/Users/jpb67",
            ]
        workflow_directory = tempfile.mkdtemp()
        cwl_path = os.path.join(workflow_directory, 'workflow.cwl')
        text_to_file(SAMPLE_WORKFLOW, cwl_path)
        cwl_file_url = "file://{}".format(cwl_path)
        workflow_object_name = ""

        input_file_directory = tempfile.mkdtemp()
        one_path, two_path = self.make_input_files(input_file_directory)
        input_json = """
files:
  - class: File
    path: {}
  - class: File
    path: {}
outputfile: results.txt
        """.format(one_path, two_path)
        workflow = CwlWorkflow(job_id,
                               working_directory,
                               output_directory,
                               cwl_base_command)
        workflow.run(cwl_file_url, workflow_object_name, input_json)
        shutil.rmtree(workflow_directory)
        shutil.rmtree(input_file_directory)

        result_file_content = file_to_text(os.path.join(working_directory, output_directory, "results.txt"))
        self.assertEqual(result_file_content, "one\ntwo\n")
        shutil.rmtree(working_directory)

    def test_simple_workflow_missing_files(self):
        """
        Make sure run raises JobStepFailed when we are missing input files.
        """
        job_id = 1
        working_directory = tempfile.mkdtemp()
        output_directory = 'result'
        cwl_base_command = None
        if platform.system() == 'Darwin':
            cwl_base_command = [
                "cwl-runner",
                "--debug",
                "--tmpdir-prefix=/Users/jpb67",
                "--tmp-outdir-prefix=/Users/jpb67",
            ]
        workflow_directory = tempfile.mkdtemp()
        cwl_path = os.path.join(workflow_directory, 'workflow.cwl')
        text_to_file(SAMPLE_WORKFLOW, cwl_path)
        cwl_file_url = "file://{}".format(cwl_path)
        workflow_object_name = ""

        input_file_directory = tempfile.mkdtemp()
        one_path, two_path = self.make_input_files(input_file_directory)
        input_json = """
        files:
          - class: File
            path: {}
          - class: File
            path: {}
        outputfile: results.txt
                """.format(one_path, two_path)
        os.unlink(one_path)
        os.unlink(two_path)
        workflow = CwlWorkflow(job_id,
                               working_directory,
                               output_directory,
                               cwl_base_command)
        with self.assertRaises(JobStepFailed):
            workflow.run(cwl_file_url, workflow_object_name, input_json)
        shutil.rmtree(workflow_directory)
        shutil.rmtree(input_file_directory)
        shutil.rmtree(working_directory)