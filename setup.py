from setuptools import setup, find_packages


LANDO_REQUIREMENTS = [
      "shade==1.29.0",
      "cwlref-runner==1.0",
      "DukeDSClient==1.0.3",
      "humanfriendly==2.4",
      "Jinja2==2.9.5",
      "kubernetes==8.0.1",
      "pyasn1<0.5.0,>=0.4.1",
      "lando-messaging==0.7.7",
      "Markdown==2.6.9",
      "python-dateutil==2.6.0",
      "PyYAML==3.12",
      "requests==2.20.1",
]

setup(name='lando',
      version='0.9.14',
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

