from __future__ import print_function, absolute_import
from lando.server.config import ServerConfig
from lando.server.lando import Lando, CONFIG_FILE_NAME

def main():
    config = ServerConfig(CONFIG_FILE_NAME)
    lando = Lando(config)
    lando.listen_for_messages()

if __name__ == "__main__":
    main()