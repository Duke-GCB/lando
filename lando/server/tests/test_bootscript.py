from __future__ import absolute_import
from unittest import TestCase
from lando.server.bootscript import BootScript

WORKER_CONFIG_YML = """data:
  one: 1
  two: 2
"""

EXPECTED_SCRIPT = """
#!/usr/bin/env bash
# Setup config file for lando_client.py
WORKER_CONFIG=/etc/lando_worker_config.yml
cat <<EOF > $WORKER_CONFIG
data:
  one: 1
  two: 2
EOF
"""


class TestServerConfig(TestCase):
    def test_building_content(self):
        """
        Test to make sure we can create the bootscript to place the lando worker config file.
        """
        boot_script = BootScript(WORKER_CONFIG_YML)
        self.assertMultiLineEqual(EXPECTED_SCRIPT.strip(), boot_script.content.strip())

