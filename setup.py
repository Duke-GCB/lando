from setuptools import setup, find_packages


LANDO_REQUIREMENTS = [
      "ruamel.yaml==0.15.2",
      "Babel==2.3.4",
      "shade==1.20.0",
      "cwlref-runner==1.0",
      "DukeDSClient==0.3.12",
      "humanfriendly==2.4",
      "Jinja2==2.9.5",
      "lando-messaging==0.7.1",
      "python-dateutil==2.6.0",
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
      zip_safe=False,
      entry_points={
            'console_scripts': [
                  'lando = lando.server.__main__:main',
                  'lando_worker = lando.worker.__main__:main',
                  'lando_client = lando.client.__main__:main',
            ]
      }
      )

