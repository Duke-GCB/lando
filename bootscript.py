"""
Creates a bash script to be run on a new vm.
This allows passing arguments and running a program.
"""
import yaml


class BootScript(object):
    def __init__(self, yaml_str, worker_config_yml, server_name):
        settings = yaml.load(yaml_str)
        self.git_url = settings.get('git_clone')
        self.working_directory = settings.get('cd')
        self.workflow_filename = settings.get('run_cwl')
        self.workflow_params = settings.get('input')
        self.workflow_params_filename = '$PARAMS_FILE'
        self.workerconfig = worker_config_yml
        self.workerconfig_filename = '$WORKER_CONFIG'
        self.server_name = server_name
        self.content = ""
        self._build_content()

    def _build_content(self):
        self._add_shebang_str()
        self._add_worker_config()
        self._add_workflow_params()
        self._add_run_workflow()
        self._add_terminate_vm()

    def _add_shebang_str(self):
        self.content += "#!/usr/bin/env bash\n"

    def _add_worker_config(self):
        self.content += "# Setup config file for lando_client.py\n"
        self.content += "WORKER_CONFIG=/tmp/workerconfig.$$.yml\n"
        self.content += self._file_with_content_str(self.workerconfig_filename, self.workerconfig)

    def _add_workflow_params(self):
        self.content += "# Setup params file for running workflow\n"
        self.content += "PARAMS_FILE=/tmp/params.$$.yml\n"
        self.content += self._file_with_content_str(self.workflow_params_filename, self.workflow_params)

    def _add_run_workflow(self):
        self.content += self.make_base_script_str()

    def _add_terminate_vm(self):
        pass

    def make_base_script_str(self):
        command_str = "/lando/lando_worker.sh {} {} {} {} {} {}\n"
        return command_str.format(self.git_url, self.working_directory,
                                  self.workflow_filename, self.workflow_params_filename,
                                  self.workerconfig_filename, self.server_name)

    @staticmethod
    def _file_with_content_str(filename, content):
        heredoc_cat = "cat <<EOF > {}\n{}EOF\n"
        return heredoc_cat.format(filename, content)

