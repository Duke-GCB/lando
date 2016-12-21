#!/bin/bash
set -e
PYTHON=/usr/local/bin/python

source dds.config

(>&2 echo "Clone and setup bespin Environment")
git clone https://github.com/Duke-GCB/bespin-api.git 2>&1
cd bespin-api
virtualenv -p $PYTHON env 2>&1
source env/bin/activate 
pip install -r requirements.txt 2>&1
python manage.py migrate 2>&1

(>&2 echo "Setup Environment")
python manage.py shell < ../setup.py
 
deactivate
cd ..
