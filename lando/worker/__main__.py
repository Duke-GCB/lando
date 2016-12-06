"""
Command line program for running the worker: lando_worker.
Reads the worker config file for all it's settings.
"""
from __future__ import print_function, absolute_import
from lando.worker.config import WorkerConfig
from lando.worker.worker import CONFIG_FILE_NAME, LandoWorker
from lando.server.lando import LANDO_QUEUE_NAME
from lando.messaging.messaging import MessageRouter


def main():
    config = WorkerConfig(CONFIG_FILE_NAME)
    worker = LandoWorker(config, outgoing_queue_name=LANDO_QUEUE_NAME)
    MessageRouter.run_worker_router(config, worker, listen_queue_name=config.queue_name)

if __name__ == "__main__":
    main()