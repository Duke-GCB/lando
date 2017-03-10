from setuptools import setup, find_packages


LANDO_REQUIREMENTS = [
      "cwlref-runner==1.0",
      "DukeDSClient",
      "Jinja2==2.9.5",
      "keystoneauth1>=2.11.0",
      "lando-messaging",
      "python-novaclient>=2.21.0,!=2.27.0,!=2.32.0",
      "PyYAML==3.11",
      "requests==2.10.0",
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
      dependency_links=['https://github.com/Duke-GCB/lando-messaging/tarball/master#egg=lando-messaging'],
      zip_safe=False,
      entry_points={
            'console_scripts': [
                  'lando = lando.server.__main__:main',
                  'lando_worker = lando.worker.__main__:main',
                  'lando_client = lando.client.__main__:main',
            ]
      }
      )

