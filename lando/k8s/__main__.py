import os
import sys
import logging
from lando.k8s.config import create_server_config
from lando.k8s.lando import K8sLando


def main():
    config = create_server_config(sys.argv[1])
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    lando = K8sLando(config)
    lando.listen_for_messages()


if __name__ == "__main__":
    main()
