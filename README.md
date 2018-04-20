# lando

> I'm the administrator of this facility

Service that runs cwl workflows on VMs in a openstack cloud.

The project is made up of 3 scripts:

- __lando__ - server that spawns VMs and sends messages for them to run job steps
- __lando_worker__ - program that runs inside the VMs that listens for messages to run different steps
- __lando_client__ - program that can send lando the start/cancel message (only used for testing purposes)

The major external components are:

- __Rabbitmq__ - a queue were messages are placed for lando and lando_worker to consume.
- __bespin-api__ - a REST API that contains data about jobs to run and will put __start\_job__ and __cancel\_job__ in the queue for __lando__. https://github.com/Duke-GCB/bespin-api
- __Openstack__ - a cloud where VMs are created and will have lando_client run in them to execute workflows.

## Message Flow

![alt text](https://github.com/Duke-GCB/lando/raw/master/lando-diagram.png "Lando Diagram")

Running job message flow (omitting Rabbitmq):

1.  __bespin-api__ posts a start_job message for __lando__

2.  __lando__ tells __Openstack__ to creates VM that runs __lando_worker__

3.  __lando__ posts a stage_job message for __lando_worker__

  1.  __lando_worker__ downloads files for the job

  2.  __lando_worker__ sends stage_job_complete to  __lando__

4.  __lando__ posts a run_job message for __lando_worker__

  1.  __lando_worker__ runs the CWL workflow for the job

  2.  __lando_worker__ sends run_job_complete to  __lando__

5.  __lando__ posts a save_output message for __lando_worker__

  1.  __lando_worker__ runs the CWL workflow for the job

  2.  __lando_worker__ sends save_output_complete to  __lando__

6.  __lando__ tells __Openstack__ to terminate the __lando_worker__ VM

Additionally __lando__ reads and updates __bespin-api__ job table as the job progresses.


## Setup
Assumes you have installed Python 2.7, [Openstack](https://www.openstack.org/), [Rabbitmq](http://www.rabbitmq.com/).

### Install lando-messaging and lando.
```
pip install git+git://github.com/Duke-GCB/lando.git
```

### Install Bespin-api.
Run the docker image or use the development instructions at https://github.com/Duke-GCB/bespin-api/blob/master/README.md

### Create a workflow and questionnaire in Bespin-api

This registers details about how to run the job in openstack (CPU/RAM, VM image, volume sizes)

See https://github.com/Duke-GCB/bespin-cwl/blob/master/scripts/post_questionnaire.sh for an example

### Create job in Bespin-api

Using the bespin superuser you created in the previous step go into the admin interface and setup a job.

### Create lando config files

There are two config files that are used by lando.
* `/etc/lando_config.yml` - this is the main configuration file used by the server program(lando).
* `/etc/lando_worker_config.yml` - this is the  configuration file used by the worker.
When using Openstack the server program creates and puts the worker's config file on the VM in the correct location.


#### Sample `/etc/lando_config.yml` file:
```
# Rabbitmq settings
work_queue:
  host: 10.109.253.74       # ip address of the rabbitmq
  username: lando           # username for lando server
  password: secret1         # password for lando server
  listen_queue: lando       # queue that lando server should listen on
  worker_username: worker   # username for lando worker
  worker_password: secret2  # password for lando worker

# General Openstack settings
cloud_settings:
  auth_url: http://10.109.252.9:5000/v3
  username: jpb67
  password: secret3
  user_domain_name: Default
  project_name: jpb67               # name of the project we will add VMs to
  project_domain_name: Default

# Bespin job API settings
bespin_api:
  url: http://localhost:8000/api
  username: jpb67
  password: secret4
```
If you are running with valid openstack credentials you will not need to create a `/etc/lando_worker_config.yml` file.
The lando service does this for you.

### Add users to Rabbitmq
```
rabbitmqctl add_user lando secret1
rabbitmqctl set_permissions -p / lando  ".*" ".*" ".*"

rabbitmqctl add_user worker secret2
rabbitmqctl set_permissions -p / worker  ".*" ".*" ".*"
```

### Running with Openstack

You can start lando by simply running `lando` where it can see the `/etc/lando_config.yml` config file.

## Running without Openstack


#### Turn on option to fake cloud service in `/etc/lando_config.yml`
At the end of `/etc/lando_config.yml` add the following:
```
fake_cloud_service: True
```
This will cause lando to print a message telling you to run lando_worker.


#### Sample `/etc/lando_worker_config.yml` file for fake cloud service:

```
host: 10.109.253.74
username: worker
password: secret2
queue_name: local_worker
```
The queue name `local_worker` is always used for workers when `fake_cloud_service` is True in `/etc/lando_config.yml`.

If you are running on osx you may need to specify custom `--tmpdir-prefix` and `--tmp-outdir-prefix` flags for cwl.
You can replace the default `cwl-runner` command by adding lines similar to these:
```
cwl_base_command:
  - "cwl-runner"
  - "--debug"
  - "--tmpdir-prefix=/Users/jpb67/Documents/work/tmp"
  - "--tmp-outdir-prefix=/Users/jpb67/Documents/work/tmp"
```

### Run lando client
This command will put a job in the rabbitmq queue for the lando server to receive.
This reads the config from `/etc/lando_config.yml`.
```
lando_client start_job 1
```
This command is just meant for testing purposes.
In a typical use case this message would be queued by bespin-api.

### Run lando server
This will listen for messages from the 'lando' rabbitmq queue.
This reads the config from `/etc/lando_config.yml`.
```
lando
```
It should display that it has received a message to run a job.
Since we have set the `fake_cloud_service: True` in `/etc/lando_config.yml` instead of trying to launch a vm
it should print this message: `Pretend we create vm: local_worker`.
Finally it should put a staging data message in the worker's queue.

### Run lando worker
This reads the config from `/etc/lando_worker_config.yml`.
It should talk back and forth with lando server staging data, running, job and storing output.
```
lando_worker
```
lando_worker should terminate once it completes the job.

