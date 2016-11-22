from unittest import TestCase
from config import Config


class TestConfigLoading(TestCase):
    def test_server_config(self):
        config = Config('tests/sample_landoconfig.yml')

        work_queue = config.work_queue_config()
        self.assertEqual('10.109.253.2', work_queue.host)
        self.assertEqual('lando', work_queue.username)
        self.assertEqual('odnal', work_queue.password)
        self.assertEqual('lobot', work_queue.worker_username)
        self.assertEqual('tobol', work_queue.worker_password)
        self.assertEqual('task-queue', work_queue.queue_name)

        self.assertMultiLineEqual("""host: 10.109.253.2
queue_name: task-queue
worker_password: tobol
worker_username: lobot
""", config.make_worker_config_yml())

        vm_settings = config.vm_settings()
        self.assertEqual('xenial_docker_cwlrunner', vm_settings.worker_image_name)
        self.assertEqual('jpb67', vm_settings.ssh_key_name)
        self.assertEqual('selfservice', vm_settings.network_name)
        self.assertEqual('ext-net', vm_settings.floating_ip_pool_name)
        self.assertEqual('m1.small', vm_settings.default_favor_name)

        cloud_settings = config.cloud_settings()
        self.assertEqual('http://10.109.252.1:5000/v3', cloud_settings.auth_url)
        self.assertEqual('jpb67', cloud_settings.username)
        self.assertEqual('Default', cloud_settings.user_domain_name)
        self.assertEqual('jpb67', cloud_settings.project_name)
        self.assertEqual('Default', cloud_settings.project_domain_name)
        self.assertEqual('cheese', cloud_settings.password)
        credentials = cloud_settings.credentials()
        self.assertEqual(credentials, {
          'auth_url': 'http://10.109.252.1:5000/v3',
          'username': 'jpb67',
          'user_domain_name': 'Default',
          'project_name': 'jpb67',
          'project_domain_name': 'Default',
          'password': 'cheese'
        })

    def test_worker_config(self):
        config = Config('tests/sample_lobotconfig.yml')
        work_queue = config.work_queue_config()
        self.assertEqual('10.109.253.2', work_queue.host)
        self.assertEqual('lobot', work_queue.worker_username)
        self.assertEqual('tobol', work_queue.worker_password)
        self.assertEqual(None, work_queue.username)
        self.assertEqual(None, work_queue.password)
        self.assertEqual('task-queue', work_queue.queue_name)

        vm_settings = config.vm_settings()
        self.assertEqual(None, vm_settings)
        cloud_settings = config.cloud_settings()
        self.assertEqual(None, cloud_settings)