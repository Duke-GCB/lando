from __future__ import absolute_import
from unittest import TestCase
from lando.server.cloudconfigscript import CloudConfigScript

class TestCloudConfigScript(TestCase):

    def test_write_file(self):
        c = CloudConfigScript()
        path = '/etc/config.yml'
        file_content = 'file-content'
        expected = """#cloud-config

write_files:
- {content: file-content, path: /etc/config.yml}
"""
        c.add_write_file(file_content, path)
        self.assertMultiLineEqual(expected, c.content)

    def test_add_volume(self):
        c = CloudConfigScript()
        c.add_volume('/dev/vdg1', '/mnt/data')
        expected = """#cloud-config

disk_setup:
  /dev/vdg: {layout: true, table_type: gpt}
fs_setup:
- {device: /dev/vdg, filesystem: ext3, partition: /dev/vdg1}
mounts:
- [/dev/vdg1, /mnt/data]
"""
        self.assertMultiLineEqual(expected, c.content)
