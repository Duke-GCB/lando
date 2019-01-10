import sys
from lando.kubeworker.config import WorkerConfig
from lando.kubeworker.worker import Worker


def main():
    worker = Worker(WorkerConfig())
    worker.run()


if __name__ == "__main__":
    main()
