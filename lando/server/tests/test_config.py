from unittest import TestCase
import os
from lando.testutil import write_temp_return_filename
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
{}

cloud_settings:
  auth_url: http://10.109.252.9:5000/v3
  username: jpb67
  user_domain_name: Default
  project_name: jpb67
  project_domain_name: Default
  password: secret

bespin_api:
  url: http://localhost:8000/api
  token: 10498124091240e
"""


class TestServerConfig(TestCase):
    def test_good_config(self):
        filename = write_temp_return_filename(GOOD_CONFIG.format(""))
        config = ServerConfig(filename)
        os.unlink(filename)
        self.assertEqual(False, config.fake_cloud_service)

        self.assertEqual("10.109.253.74", config.work_queue_config.host)
        self.assertEqual("lando", config.work_queue_config.listen_queue)

        self.assertEqual('lando_worker', config.vm_settings.worker_image_name)
        self.assertEqual('jpb67', config.vm_settings.ssh_key_name)
        #  by default allocate_floating_ips is off
        self.assertEqual(False, config.vm_settings.allocate_floating_ips)

        self.assertEqual("http://10.109.252.9:5000/v3", config.cloud_settings.auth_url)
        self.assertEqual("jpb67", config.cloud_settings.username)

        self.assertEqual("http://localhost:8000/api", config.bespin_api_settings.url)
        self.assertEqual("10498124091240e", config.bespin_api_settings.token)

    def test_allocate_floating_ip_true(self):
        line = "  allocate_floating_ips: true"
        filename = write_temp_return_filename(GOOD_CONFIG.format(line))
        config = ServerConfig(filename)
        os.unlink(filename)
        self.assertEqual(True, config.vm_settings.allocate_floating_ips)

    def test_allocate_floating_ip_false(self):
        line = "  allocate_floating_ips: false"
        filename = write_temp_return_filename(GOOD_CONFIG.format(line))
        config = ServerConfig(filename)
        os.unlink(filename)
        self.assertEqual(False, config.vm_settings.allocate_floating_ips)

    def test_good_config_with_fake_cloud_service(self):
        config_data = GOOD_CONFIG.format("") + "\nfake_cloud_service: True"
        filename = write_temp_return_filename(config_data)
        config = ServerConfig(filename)
        os.unlink(filename)
        self.assertEqual(True, config.fake_cloud_service)

    def test_empty_config_file(self):
        filename = write_temp_return_filename('')
        with self.assertRaises(InvalidConfigException):
            config = ServerConfig(filename)
        os.unlink(filename)

    def test_bogus_config_file(self):
        filename = write_temp_return_filename('stuff: one')
        with self.assertRaises(InvalidConfigException):
            config = ServerConfig(filename)
        os.unlink(filename)

    def test_make_worker_config_yml(self):
        filename = write_temp_return_filename(GOOD_CONFIG.format(""))
        config = ServerConfig(filename)
        os.unlink(filename)
        expected = """
cwl_base_command: null
host: 10.109.253.74
password: tobol
queue_name: worker_1
username: lobot
"""
        result = config.make_worker_config_yml('worker_1')
        self.assertMultiLineEqual(expected.strip(), result.strip())

    def test_make_worker_config_yml_custom_cwl_base_command(self):
        line = '  cwl_base_command:\n  - "cwltoil"\n  - "--not-strict"'
        filename = write_temp_return_filename(GOOD_CONFIG.format(line))
        config = ServerConfig(filename)
        os.unlink(filename)
        expected = """
cwl_base_command:
- cwltoil
- --not-strict
host: 10.109.253.74
password: tobol
queue_name: worker_1
username: lobot
"""
        result = config.make_worker_config_yml('worker_1')
        self.assertMultiLineEqual(expected.strip(), result.strip())
