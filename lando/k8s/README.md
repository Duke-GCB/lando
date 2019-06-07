# Kubernetes Lando
This module provides support for running Bespin jobs via a k8s cluster. This consists of two services. 
- `k8s.lando` which listens for messages and runs k8s jobs while updating bespin-api
- `k8s.watcher` which watches the k8s job listing and updates lando when jobs have completed, and logs failed jobs

This requires bespin-api, postgres, and rabbitmq services to be running as outlined in [External services](#external-services) below.

### External Cloud Support
This module is designed to allow running workflows on an external kubernetes cloud without permissions to connect directly to the bespin cluster(rabbitmq and bespin-api). This allows the main application (bespin-api, rabbit, etc) to run on a private network while we can utilize an external k8s cluster for computation purposes. Part of the responsibility of `k8s.watcher` is to poll the external k8s cloud for finished jobs and add messages to the rabbitmq queue for lando to continue running the workflow.
For the local openstack cloud the lando worker directly posts these messages to the rabbitmq queue for lando so there is no need for this watcher.

### Job Monitoring
The `k8s.watcher` is also necessary to monitor jobs due to the way kubernetes jobs are created/retried in the background. The k8s api returns immediately even if the job may fail to run for some reason. So a k8s job may fail without running any code and cannot report back to `k8s.lando` that the job has failed. Lando does not watch these jobs directly due to it's responsibility to watch the rabbiitmq queue.

## Setup

### k8s Cluster setup
Connect to your k8s cluster.

Create a project where the jobs will be run
```
oc new-project lando-job-runner
```

Create a service account that will be used by k8s.lando to create jobs.
```
oc create sa lando
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

Create a DukeDS agent secret that will be used to stage data and save output.
Create a file containing your ddsclient config named `ddsclient.conf`.
Use this file to populate the DukeDS secret for your agent.
```
oc create secret generic ddsclient-agent --from-file=config=ddsclient.conf
```

Setup roles for use by calrissian and lando service account
```
oc create role pod-manager-role --verb=create,patch,delete,list,watch --resource=pods
oc create role log-reader-role --verb=get,list --resource=pods/log
oc create role pvc-manager-role --verb=create,delete,list,watch --resource=pvc
oc create role configmaps-manager-role --verb=create,delete,list,watch --resource=configmaps
oc create role job-manager-role --verb=create,delete,list,watch --resource=jobs.batch
```

Bind roles required for the CWL workflow running image (calrissian)
```
oc create rolebinding pod-manager-default-binding --role=pod-manager-role --serviceaccount=lando-job-runner:default
oc create rolebinding log-reader-default-binding --role=log-reader-role --serviceaccount=lando-job-runner:default
```

Bind roles necessary to lando service account
```
oc create rolebinding lando-pvc-manager-role-binding --role=pvc-manager-role --serviceaccount=lando-job-runner:lando
oc create rolebinding lando-configmaps-manager-role-binding --role=configmaps-manager-role --serviceaccount=lando-job-runner:lando
oc create rolebinding lando-job-manager-role-binding --role=job-manager-role --serviceaccount=lando-job-runner:lando
oc create rolebinding lando-pod-manager-role-binding --role=pod-manager-role --serviceaccount=lando-job-runner:lando
oc create rolebinding lando-log-reader-role-binding --role=log-reader-role --serviceaccount=lando-job-runner:lando
```

Build the lando-util image that will be used for the stage data, organize output, and upload results jobs.
```
oc create -f https://raw.githubusercontent.com/Duke-GCB/lando-util/master/openshift/BuildConfig.yml
```

Create a persistent volume for holding system data matching the name in `run_workflow_settings.volume_claim_name` from the config file below.

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

run_workflow_settings:
  system_data_volume:
     volume_claim_name: system-data
     mount_path: "/bespin/system/"

data_store_settings:
  secret_name: ddsclient-agent

storage_class_name: glusterfs-storage

log_level: INFO
```

### External services

You will need to setup [bespin-api](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_web/tasks),
[postgres](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_database/tasks) and [rabbitmq](https://github.com/Duke-GCB/gcb-ansible-roles/tree/master/bespin_rabbit/tasks) first to interact with k8s lando.

`bespin-api` will need to have JobRuntimeK8s/JobRuntimeStepK8s with the appropriate image name and base commands setup
```
- image name: `dukegcb:calrissian:latest`
- base command: `["calrissian", "--max-ram", "16384", "--max-cores", "8"]`
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
