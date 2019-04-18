import os
import sys
from setuptools import setup, find_packages
# version checking derived from https://github.com/levlaz/circleci.py/blob/master/setup.py
from setuptools.command.install import install

VERSION = '0.9.18'
TAG_ENV_VAR = 'CIRCLE_TAG'


LANDO_REQUIREMENTS = [
      "shade==1.29.0",
      "DukeDSClient==2.1.4",
      "humanfriendly==2.4",
      "Jinja2==2.10.1",
      "kubernetes==8.0.1",
      "pyasn1<0.5.0,>=0.4.1",
      "lando-messaging==1.0.0",
      "Markdown==2.6.9",
      "python-dateutil==2.6.0",
      "PyYAML==5.1",
      "requests==2.20.1",
]


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv(TAG_ENV_VAR)

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)


setup(name='lando',
      version=VERSION,
      description='Cloud based bioinformatics workflow runner',
      url='https://github.com/Duke-GCB/lando',
      author='Dan Leehr, John Bradley',
      author_email='john.bradley@duke.edu',
      license='MIT',
      packages=find_packages(),
      install_requires=LANDO_REQUIREMENTS,
      zip_safe=False,
      entry_points={
            'console_scripts': [
                  'lando = lando.server.__main__:main',
                  'lando_worker = lando.worker.__main__:main',
                  'lando_client = lando.client.__main__:main',
            ]
      },
      cmdclass={
          'verify': VerifyVersionCommand,
      },
)

