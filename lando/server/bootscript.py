"""
Creates a bash script to be run on a new vm.
This script writes out the worker config file for use by the lando_worker program.
lando_worker is expected to be setup as a service.
"""


class BootScript(object):
    """
    Creates a bash script that will start a lando_worker.
    """
    def __init__(self, worker_config_yml):
        """
        Creates a bash script that will create the worker config file to be used by lando_worker.
        Property content contains the script contents.
        :param worker_config_text: str: contents of the worker config file
        """
        self.workerconfig = worker_config_yml
        self.workerconfig_filename = '$WORKER_CONFIG'
        self.content = ""
        self._build_content()

    def _build_content(self):
        self._add_shebang_str()
        self._add_worker_config()

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
        self.content += "WORKER_CONFIG=/etc/lando_worker_config.yml\n"
        self.content += self._file_with_content_str(self.workerconfig_filename, self.workerconfig)

    @staticmethod
    def _file_with_content_str(filename, content):
        """
        Return bash string that puts content into filename.
        :param filename: str: name of the file(environment variable) we want to store data into
        :param content: str: settings to be stored in the file
        """
        heredoc_cat = "cat <<EOF > {}\n{}EOF\n"
        return heredoc_cat.format(filename, content)

