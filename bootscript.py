"""
Creates a bash script to be run on a new vm.
This allows passing arguments and running a program.
"""
import yaml


class BootScript(object):
    """
    Creates a bash script that will start a lando_worker.
    """
    def __init__(self, worker_config_yml, server_name):
        """
        Fills in content property with script based on passed in settings.
        :param worker_config_yml: str: text in yaml format to be stored in the worker config file
        :param server_name: str: our name that should be sent with every message we send
        """
        self.workerconfig = worker_config_yml
        self.workerconfig_filename = '$WORKER_CONFIG'
        self.server_name = server_name
        self.content = ""
        self._build_content()

    def _build_content(self):
        self._add_shebang_str()
        self._add_worker_config()
        self._add_run_lando_client()

    def _add_shebang_str(self):
        """
        Add shebang to the script content.
        """
        self.content += "#!/usr/bin/env bash\n"

    def _add_worker_config(self):
        """
        Add heredoc to create $WORKER_CONFIG file.
        This file is used to talk to the work queue(AMQP).
        """
        self.content += "# Setup config file for lando_client.py\n"
        self.content += "WORKER_CONFIG=/tmp/workerconfig.$$.yml\n"
        self.content += self._file_with_content_str(self.workerconfig_filename, self.workerconfig)

    def _add_run_lando_client(self):
        """
        Runs lando_worker.sh which calls cwl-runner
        """
        self.content += self.make_base_script_str()

    def make_base_script_str(self):
        """
        Return a string that calls lando_worker.sh with all necessary arguments.
        """
        command_str = "cd /lando && python lando_client.py {}\n"
        return command_str.format(self.server_name)

    @staticmethod
    def _file_with_content_str(filename, content):
        """
        Return bash string that puts content into filename.
        :param filename: str: name of the file(environment variable) we want to store data into
        :param content: str: settings to be stored in the file
        """
        heredoc_cat = "cat <<EOF > {}\n{}EOF\n"
        return heredoc_cat.format(filename, content)

