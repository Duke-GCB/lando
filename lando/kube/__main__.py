import os
import sys
import logging
from lando.kube.config import ServerConfig
from lando.kube.lando import K8sLando, CONFIG_FILE_NAME


def main():
    config_filename = os.environ.get("LANDO_CONFIG")
    if not config_filename:
        config_filename = CONFIG_FILE_NAME
    config = ServerConfig(config_filename)
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    lando = K8sLando(config)
    lando.listen_for_messages()


if __name__ == "__main__":
    main()
