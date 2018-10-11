from setuptools import setup, find_packages


LANDO_REQUIREMENTS = [
      "shade==1.29.0",
      "cwlref-runner==1.0",
      "DukeDSClient==1.0.3",
      "humanfriendly==2.4",
      "Jinja2==2.9.5",
      "lando-messaging==0.7.4",
      "Markdown==2.6.9",
      "python-dateutil==2.6.0",
      "PyYAML>3,<4",
      "requests==2.18.1",
]

setup(name='lando',
      version='0.9.2',
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

