"""
Command line program for running the worker: lando_worker.
Reads the worker config file for all it's settings.
"""
from __future__ import print_function, absolute_import
import os
from lando.worker.config import WorkerConfig
from lando.worker.worker import CONFIG_FILE_NAME, LandoWorker
from lando.server.lando import LANDO_QUEUE_NAME


def main():
    config_filename = os.environ.get("LANDO_WORKER_CONFIG")
    if not config_filename:
        config_filename = CONFIG_FILE_NAME
    config = WorkerConfig(config_filename)
    worker = LandoWorker(config, outgoing_queue_name=LANDO_QUEUE_NAME)
    worker.listen_for_messages()

if __name__ == "__main__":
    main()
