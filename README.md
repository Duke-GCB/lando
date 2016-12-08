# lando

> I'm the administrator of this facility

Service that runs cwl workflows on VMs in a openstack cloud.

## Setup
Assumes you have installed Python 2.7, [Openstack](https://www.openstack.org/), [Rabbitmq](http://www.rabbitmq.com/).

### Install lando-messaging and lando.
```
pip install git+git://github.com/Duke-GCB/lando-messaging.git 
pip install git+git://github.com/Duke-GCB/lando.git@use_bespin_api
```

### Install Bespin-workflows.
Follow the instructions to install the `lando_api` branch:
https://github.com/Duke-GCB/bespin-workflows/blob/lando_api/README.md

### Create lando config files
There are two config files that are used by lando.
`/etc/lando_config.yml` - this is the main configuration file used by the server program(lando).
`/etc/lando_worker_config.yml` - this is the  configuration file used by the worker.
When using Openstack the server program creates and puts the worker's config file on the VM in the correct location.

Sample `/etc/lando_config.yml` file:
```
work_queue:
  host: 10.109.253.74
  username: lando
  password: secret1
  worker_username: lobot
  worker_password: secret2
  listen_queue: lando

vm_settings:
  worker_image_name: lando_worker
  ssh_key_name: jpb67
  network_name: selfservice
  floating_ip_pool_name: ext-net
  default_favor_name: m1.small

cloud_settings:
  auth_url: http://10.109.252.9:5000/v3
  username: jpb67
  user_domain_name: Default
  project_name: jpb67
  project_domain_name: Default
  password: secret3

job_api:
  url: http://localhost:8000/api
  username: jpb67
  password: secret4
```


## Running without Openstack



### Run lando client 
This command will put a job in the rabbitmq queue for the lando server to receive.
This reads the config from `/etc/lando_config.yml`.
```
lando_client start_job 1
```

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
