"""
Server that listens for request to run/cancel jobs.
Starts VMs and has them run various job stages.
"""
from __future__ import print_function, absolute_import
import os
from lando.server.config import ServerConfig
from lando.server.lando import Lando, CONFIG_FILE_NAME


def main():
    config_filename = os.environ.get("LANDO_CONFIG")
    if not config_filename:
        config_filename = CONFIG_FILE_NAME
    config = ServerConfig(config_filename)
    lando = Lando(config)
    lando.listen_for_messages()

if __name__ == "__main__":
    main()