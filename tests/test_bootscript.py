from unittest import TestCase
from bootscript import BootScript


class TestBootScript(TestCase):
    def test_parsed_config(self):
        input_yaml = """
        git_clone: https://github.com/johnbradley/toyworkflow.git
        cd: toyworkflow
        input: |
          dds_agent_key: randomletters1
          dds_user_key: randomelettser2
          projectName: dizzycat
          input_files: [hey.txt, hey2.txt]
          output_file: results.txt
        run_cwl: mycat-workflow.cwl
        workerconfig: |
          work_queue:
            host: 10.109.253.74
            username: lobot
            password: tobol
            queue_name: task-queue
        """
        worker_config_yml = """work_queue:
  host: 10.109.253.2
  worker_username: lobot
  worker_password: tobol
  queue_name: dataqueue
"""
        expected = """#!/usr/bin/env bash
# Setup config file for lando_client.py
WORKER_CONFIG=/tmp/workerconfig.$$.yml
cat <<EOF > $WORKER_CONFIG
work_queue:
  host: 10.109.253.2
  worker_username: lobot
  worker_password: tobol
  queue_name: dataqueue
EOF
# Setup params file for running workflow
PARAMS_FILE=/tmp/params.$$.yml
cat <<EOF > $PARAMS_FILE
dds_agent_key: randomletters1
dds_user_key: randomelettser2
projectName: dizzycat
input_files: [hey.txt, hey2.txt]
output_file: results.txt
EOF
/lando/lando_worker.sh https://github.com/johnbradley/toyworkflow.git toyworkflow mycat-workflow.cwl $PARAMS_FILE $WORKER_CONFIG worker_123
"""
        boot_script = BootScript(yaml_str=input_yaml, worker_config_yml=worker_config_yml, server_name="worker_123")
        self.maxDiff = None
        self.assertMultiLineEqual(expected, boot_script.content)
