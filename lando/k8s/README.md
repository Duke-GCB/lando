# Kubernetes Lando
This module provides support for running Bespin jobs via a k8s cluster.

## Setup

### Cluster setup
Connect to your k8s cluster.

Create a project
```
oc new-project lando-job-runner
```

Create a service account
```
oc create sa lando
```

Give this account admin priv (for now)
```
oc create rolebinding lando-binding --clusterrole=admin --serviceaccount=lando-job-runner:lando
```

Find the name of a token for this service account
```
oc describe sa lando
```

Determine the token value:
```
oc describe secret <tokename>
```
This value will need to be added to your k8s lando config file under `cluster_api_settings.token`.

Create a DukeDS agent secret
Create a file containing your ddsclient config named `ddsclient.conf`.
Use this file to populate the DukeDS secret for your agent.
```
oc create secret generic ddsclient-agent --from-file=config=ddsclient.cred
```

Build the calrissian image
```
oc create -f https://raw.githubusercontent.com/Duke-GCB/calrissian/master/openshift/BuildConfig.yaml
oc create role pod-manager-role --verb=create,delete,list,watch --resource=pods
oc create rolebinding pod-manager-default-binding --role=pod-manager-role --serviceaccount=lando-job-runner:default
```

Build the lando-util image
```
oc create -f https://raw.githubusercontent.com/Duke-GCB/lando-util/master/openshift/BuildConfig.yml
```

If desired create a persistent volume for holding system data.

### Config file setup
Create a config file named `k8s.config`.
Example content:
```
work_queue:
  host: TODO
  username: TODO
  password: TODO
  worker_username: TODO
  worker_password: TODO
  listen_queue: TODO

cluster_api_settings:
  host: TODO
  token: TODO
  namespace: lando-job-runner
  verify_ssl: False

bespin_api:
  token: TODO
  url: TODO

stage_data_settings:
  image_name: lando-util:latest
  command:
    - python
    - -m
    - lando_util.download
  env_dict:
  requested_cpu: 1
  requested_memory: 1G

run_workflow_settings:
  requested_cpu: 1
  requested_memory: 2G
  system_data_volume:
     volume_claim_name: system-data
     mount_path: "/bespin/system/"

organize_output_settings:
  image_name: lando-util:latest
  command:
    - python
    - -m
    - lando_util.organize_project
  requested_cpu: 1
  requested_memory: 256M

save_output_settings:
  image_name: lando-util:latest
  command:
    - python
    - -m
    - lando_util.upload
  requested_cpu: 1
  requested_memory: 1G

data_store_settings:
  secret_name: ddsclient-agent

storage_class_name: glusterfs-storage

log_level: INFO
```

### External services

You will need to setup [bespin-api](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_web/tasks),
[postgres](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_database/tasks) and [rabbitmq](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_rabbit/tasks) first to interact with k8s lando.

`bespin-api` will need a vm settings with the appropriate image name and cwl base command
```
- image name: `calrissian:latest`
- cwl base command: `["python", "-m", "calrissian.main", "--max-ram", "16384", "--max-cores", "8"]`
```


## Running
In one terminal run the k8s watcher
```
python -m lando.k8s.watcher k8s.config
```

In another terminal run k8s lando
```
python -m lando.k8s.lando k8s.config
```

Then start a job via bespin-api.
