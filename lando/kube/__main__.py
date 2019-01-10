import os
import sys
import logging
from lando.kube.config import ServerConfig
from lando.kube.lando import K8sLando


def main():
    config = ServerConfig()
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    lando = K8sLando(config)
    lando.listen_for_messages()


if __name__ == "__main__":
    main()
