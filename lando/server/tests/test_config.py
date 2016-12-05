from unittest import TestCase
import os
import tempfile
from lando.server.config import ServerConfig
from lando.exceptions import InvalidConfigException

GOOD_CONFIG = """
work_queue:
  host: 10.109.253.74
  username: lando
  password: odnal
  worker_username: lobot
  worker_password: tobol
  listen_queue: lando

vm_settings:
  worker_image_name: lando_worker
  ssh_key_name: jpb67
  network_name: selfservice
  floating_ip_pool_name: ext-net
  default_favor_name: m1.small

cloud_settings:
  auth_url: http://10.109.252.9:5000/v3
  username: jpb67
  user_domain_name: Default
  project_name: jpb67
  project_domain_name: Default
  password: secret

job_api:
  url: http://localhost:8000/api
  username: jpb67
  password: secret
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


class TestServerConfig(TestCase):
    def test_good_config(self):
        filename = write_temp_return_filename(GOOD_CONFIG)
        config = ServerConfig(filename)
        os.unlink(filename)
        work_queue_config = config.work_queue_config()
        self.assertEqual("10.109.253.74", work_queue_config.host)
        self.assertEqual("lando", work_queue_config.listen_queue)
