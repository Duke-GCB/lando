from __future__ import print_function, absolute_import
from lando.server.config import ServerConfig
from lando.server.lando import Lando, CONFIG_FILE_NAME

if __name__ == "__main__":
    config = ServerConfig(CONFIG_FILE_NAME)
    lando = Lando(config)
    lando.listen_for_messages()