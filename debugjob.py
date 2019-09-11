# Prints out k8s yaml to setup for allowing a user to debug a job.
# Meant to be used with kubectl:
# python debugjob.py | kubectl create -n lando-job-runner -f -
from jinja2 import Template
import yaml

CONFIG_FILENAME = "debugjob.yml"
with open(CONFIG_FILENAME) as inconfig:
    config = yaml.safe_load(inconfig)

job_id = config['job_id']
job_username = config['job_username']
app_name = "debug-job-{}".format(job_id)
ingres_host = app_name + config['ingres_host_suffix']

# Names and mount points for PVCs to be mounted (based on lando code)
output_data_pvc_name = 'output-data-{}-{}'.format(job_id, job_username)
output_data_mount_path = '/bespin/output-data'
job_data_pvc_name = 'job-data-{}-{}'.format(job_id, job_username)
job_data_mount_path = '/bespin/job-data'
system_data_pvc_name = 'system-data'
system_data_mount_path = '/bespin/system-data'

with open(config['template_filename']) as infile:
    template = Template(infile.read())
    print(template.render(
        app_name=app_name,
        ingress_host=ingres_host,
        image_name=config['notebook_image'],
        notebook_password=config['notebook_password'],
        public_key=config['public_key'],
        output_data_pvc_name=output_data_pvc_name,
        output_data_mount_path=output_data_mount_path,
        job_data_pvc_name=job_data_pvc_name,
        job_data_mount_path=job_data_mount_path,
        system_data_pvc_name=system_data_pvc_name,
        system_data_mount_path=system_data_mount_path,
    ))
