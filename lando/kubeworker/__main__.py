import sys
import logging
from lando.kubeworker.config import WorkerConfig
from lando.kubeworker.worker import Worker


def main():
    config = WorkerConfig()
    worker = Worker(config)
    logging.basicConfig(stream=sys.stdout, level=config.log_level)
    worker.run()


if __name__ == "__main__":
    main()
