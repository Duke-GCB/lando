# Big Picture
System for running cwl workflows in openstack cloud.
1. A rabbitmq queue that holds launch and terminate instance messages
2. lando.py server that processes messages from the queue creating and terminating VMs
3. lando_client.py program that adds a new message to the queue containing all data necessary to run the workflows

# Walkthrough
User has a CWL workflow that downloads files, runs processes and saves the results.
I have some samples CWL workflows here: https://github.com/johnbradley/toyworkflow.git

## Start lando.py server
Copy landoconfig.sample to landoconfig.yml and fill in settings.
Launch the server:
```
python lando.py
```
This will hang until it is killed or sent the `shutdown_server` command.

## Create job yaml config file
This file has 4 required properties.
* `git_clone` - url to git repo we will clone
* `cd` - directory inside of the repo we will cd into
* `input` - content of the yaml properties file cwl-runner will be passed
* `run_cwl` - name of the workflow we want to run

Example to run fastqc on a fastq file in a DukeDS repo I put into a file named `fastqc-job.yml`:
```
git_clone: https://github.com/johnbradley/toyworkflow.git
cd: toyworkflow
input: |
  dds_agent_key: <somekey>
  dds_user_key: <somekey>
  projectName: MouseRNA
  input_file: ERR550644_1.fastq
  output_file: ERR550644_1_fastqc.zip
run_cwl: fastqc-workflow.cwl
```

## Send job to a queue with lando_client.py
Create workerconfig.yml based on landoconfig.yml.
This script has many positional arguments that take the form:
```
python lando_client.py <config_filename> <command> <job_yaml_filename> 
```
So to request `fastqc-job.yml` from earlier:
```
python lando_client.py workerconfig.yml start_worker fastqc-job.yml 
```

## Job flow
At this point `lando.py` should receive the message.
A VM will be created in openstack and a startup script will be run.
This VM will already have the lando source code installed at `/lando`.
This startup script will 
1. create a workerconfig.yml file for communicating with the queue.
2. create a cwl parameter yaml file for use with cwl-runner
3. run `/lando/lando_worker.sh`. This script will run the cwl workflow based on arguments passed in then queue a message to terminate the VM.




_This isn't meant to be the final way this all works._
