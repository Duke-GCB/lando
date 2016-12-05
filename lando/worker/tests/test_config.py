from unittest import TestCase
import os
import tempfile
from lando.worker.config import WorkerConfig
from lando.exceptions import InvalidConfigException

GOOD_CONFIG = """
host: 10.109.253.74
username: worker
password: workerpass
queue_name: task-queue
"""

# missing queuename field
BAD_CONFIG = """
host: 10.109.253.74
username: worker
password: workerpass
"""

def write_temp_return_filename(data):
    """
    Write out data to a temporary file and return that file's name.
    :param data: str: data to be written to a file
    :return: str: temp filename we just created
    """
    file = tempfile.NamedTemporaryFile(delete=False)
    file.write(data)
    file.close()
    return file.name


class TestWorkerConfig(TestCase):
    def test_good_config(self):
        filename = write_temp_return_filename(GOOD_CONFIG)
        config = WorkerConfig(filename)
        os.unlink(filename)
        self.assertEqual("10.109.253.74", config.host)
        self.assertEqual("worker", config.username)
        self.assertEqual("workerpass", config.password)
        self.assertEqual("task-queue", config.queue_name)

    def test_empty_config(self):
        filename = write_temp_return_filename("")
        with self.assertRaises(InvalidConfigException):
            config = WorkerConfig(filename)
        os.unlink(filename)


    def test_bad_config(self):
        filename = write_temp_return_filename(BAD_CONFIG)
        with self.assertRaises(InvalidConfigException):
            config = WorkerConfig(filename)
        os.unlink(filename)
