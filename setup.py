from setuptools import setup, find_packages


LANDO_REQUIREMENTS = [
      "appdirs==1.4.0",
      "Babel==2.3.4",
      "backports.shutil-get-terminal-size==1.0.0",
      "cliff==2.4.0",
      "cmd2==0.6.9",
      "cwlref-runner==1.0",
      "debtcollector==1.11.0",
      "decorator==4.0.11",
      "DukeDSClient",
      "funcsigs==1.0.2",
      "functools32==3.2.3.post2",
      "ipython==5.2.1",
      "ipython-genutils==0.1.0",
      "iso8601==0.1.11",
      "jsonpatch==1.15",
      "jsonpointer==1.10",
      "jsonschema==2.5.1",
      "keystoneauth1==2.18.0",
      "lando_messaging",
      "monotonic==1.2",
      "msgpack-python==0.4.8",
      "netaddr==0.7.19",
      "netifaces==0.10.5",
      "openstacksdk==0.9.13",
      "os-client-config==1.26.0",
      "oslo.config==3.22.0",
      "oslo.i18n==3.12.0",
      "oslo.serialization==2.16.0",
      "oslo.utils==3.22.0",
      "pathlib2==2.1.0",
      "pbr==1.10.0",
      "pexpect==4.2.0",
      "pickleshare==0.7.2",
      "positional==1.1.1",
      "prettytable==0.7.2",
      "prompt-toolkit==1.0.10",
      "ptyprocess==0.5.1",
      "Pygments==2.1.3",
      "pyparsing==2.1.10",
      "python-cinderclient==1.11.0",
      "python-glanceclient==2.6.0",
      "python-keystoneclient==3.10.0",
      "python-novaclient==7.1.0",
      "python-openstackclient==3.8.1",
      "pytz==2016.6",
      "PyYAML==3.11",
      "requests==2.10.0",
      "requestsexceptions==1.1.3",
      "rfc3986==0.3.1",
      "simplegeneric==0.8.1",
      "simplejson==3.8.2",
      "six==1.10.0",
      "stevedore==1.20.0",
      "traitlets==4.2.2",
      "unicodecsv==0.14.1",
      "warlock==1.2.0",
      "wcwidth==0.1.7",
      "wrapt==1.10.8",
]

setup(name='lando',
      version='0.1',
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
      }
      )

