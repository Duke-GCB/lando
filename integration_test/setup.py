from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from data.models import DDSEndpoint, DDSUserCredential, Workflow, WorkflowVersion, Job
from data.models import JobOutputDir, JobInputFile, DDSJobInputFile, URLJobInputFile
from data.models import LandoConnection
import os

DDS_PROJECT_ID = os.environ['DDS_PROJECT_ID']
SEQ_DDS_FILE_ID = os.environ['SEQ_DDS_FILE_ID']
AGENT_KEY = os.environ['AGENT_KEY']
USER_KEY = os.environ['USER_KEY']

LANDO_USERNAME='lando'
LANDO_PASSWORD='secret'
WORKER_USERNAME='lando'
WORKER_PASSWORD='secret'

WORKFLOW_URL = 'https://raw.githubusercontent.com/johnbradley/iMADS-worker/master/predict_service/predict-workflow-packed.cwl'
MODEL1_URL='https://swift.oit.duke.edu/v1/AUTH_gcb/gordan_models/E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGC_1a2a3mer_format.model'
MODEL2_URL='https://swift.oit.duke.edu/v1/AUTH_gcb/gordan_models/E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGG_1a2a3mer_format.model'
API_ROOT='https://api.dataservice.duke.edu/api/v1'
JOB_ORDER = """
{
  "sequence": {
    "class": "File",
    "path": "sequence.fa"
  },
  "models": [
    {
      "class": "File",
      "path": "E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGC_1a2a3mer_format.model"
    },
    {
      "class": "File",
      "path": "E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGG_1a2a3mer_format.model"
    }
  ],
  "cores": [
    "GCGC",
    "GCGG"
  ],
  "width": 20,
  "kmers": [
    1,
    2,
    3
  ],
  "slope_intercept": false,
  "transform": true,
  "filter_threshold": 0.1933,
  "core_start": null,
  "output_filename": "E2f1_workflow.bed"
}
"""


user = User.objects.create_superuser(LANDO_USERNAME, '', LANDO_PASSWORD)
token = Token.objects.create(user=user)
workflow_name = 'iMADS DNA TF Prediction'
workflow = Workflow.objects.create(name=workflow_name)
workflow_version = WorkflowVersion.objects.create(workflow=workflow, version='1', url=WORKFLOW_URL)
ddsendpoint = DDSEndpoint.objects.create(name='DukeDS', agent_key=AGENT_KEY, api_root=API_ROOT)
user_cred = DDSUserCredential.objects.create(endpoint=ddsendpoint, user=user, token=USER_KEY)
job = Job.objects.create(workflow_version=workflow_version, user=user, vm_project_name='bespin_user1', job_order=JOB_ORDER)
job_output_dir = JobOutputDir.objects.create(job=job, dir_name='results', project_id=DDS_PROJECT_ID, dds_user_credentials=user_cred)

sequence_input_file = JobInputFile.objects.create(job=job, file_type=JobInputFile.DUKE_DS_FILE, workflow_name='sequence')
DDSJobInputFile.objects.create(job_input_file=sequence_input_file, project_id=DDS_PROJECT_ID, file_id=SEQ_DDS_FILE_ID, dds_user_credentials=user_cred,
      destination_path='sequence.fa', index='1')
models_input_file = JobInputFile.objects.create(job=job, file_type=JobInputFile.URL_FILE_ARRAY, workflow_name='models')
URLJobInputFile.objects.create(job_input_file=models_input_file, url=MODEL1_URL,
    destination_path='E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGC_1a2a3mer_format.model',
    index='1')
URLJobInputFile.objects.create(job_input_file=models_input_file, url=MODEL2_URL,
    destination_path='E2F1_250nM_Bound_filtered_normalized_logistic_transformed_20bp_GCGG_1a2a3mer_format.model',
    index='2')

LandoConnection.objects.create(host='127.0.0.1', username=LANDO_USERNAME, password=LANDO_PASSWORD, queue_name='lando')


LANDO_CONFIG="""
work_queue:
  host: 127.0.0.1
  username: {}
  password: {}
  worker_username: {}
  worker_password: {}
  listen_queue: lando

bespin_api:
  url: http://localhost:8000/api
  token: {}

fake_cloud_service: True
""".format(LANDO_USERNAME, LANDO_PASSWORD, WORKER_USERNAME, WORKER_PASSWORD, token.key)

with open("../lando_config.yml", 'w') as outfile:
  outfile.write(LANDO_CONFIG)

print "\n\n\nDONE"
