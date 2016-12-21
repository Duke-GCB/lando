
# Integration Testing
Allows testing lando with the exception of Openstack.
Exercises basic functionality that uses rabbitmq, bespin-api and DukeDS.

## Initial Setup

### Create project to hold an input file and results
Create a project on DukeDS and upload a DNA sequence file. https://dataservice.duke.edu
Record the project uuid, file uuid and agent/user keys.
In a terminal in this directory(integration_test).
Copy example.dds.config to dds.config.
Using data from data service fill in the following variables in dds.config: 
```
export DDS_PROJECT_ID='__FILL_IN__' 
export SEQ_DDS_FILE_ID='__FILL_IN__'
export AGENT_KEY='__FILL_IN__'
export USER_KEY='__FILL_IN__'
```
If running on OSX add `tmpdir-prefix` and `tmp-outdir-prefix` to cwl_base_command:
```
cwl_base_command:
  - "cwl-runner"
  - "--debug"
  - "--tmpdir-prefix=/Users/<username>/tmp"
  - "--tmp-outdir-prefix=/Users/<username>/tmp"
```

### Checkout bespin-api 
In a terminal in this directory(integration_test).
Clone bespin-api and setup config files:
```
./setup.sh
```
This should create the bespin-api directory containing a database with one job.
 

### Start bespin-api and rabbitmq:
In a terminal in this directory(integration_test).
Run this:
```
./start.sh
```
This will hang until you press CONTROL-C.
At this point you can navigate to http://127.0.0.1:8000/api/jobs/1/ and see the job.
You will need username:lando password:secret.

### Run lando
In another terminal in the root directory of this repo:
```
virtualenv env
source env/bin/activate
pip install git+git://github.com/Duke-GCB/lando-messaging.git 
python setup.py install 
export LANDO_CONFIG=./integration_test/lando_config.yml
lando
```
This will listen for jobs to run.

### Start the job:
Navigate to http://127.0.0.1:8000/api/jobs/1/start/ click POST.
This should notify lando who will communicate with lando_worker
to run your job.
You should see `Pretend we create vm: local_worker` on the terminal where you ran lando.


### Run lando_worker
In another terminal in the root directory of this repo:
```
source env/bin/activate
export LANDO_WORKER_CONFIG=./integration_test/worker_config.yml 
lando_worker
```
This will send the worker ready message to lando and they will begin
running the job steps. lando_worker will exit when lando terminates
the queue lando_worker is listening on.



