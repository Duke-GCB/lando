### Rough outline for messages flowing through the system

1. webserver posts run_job: 1 to queue
2. lando receives run_job: 1
   * GET webserver/api/jobs/1/?  (vm settings)
   * POST webserver/api/jobs/1 status=SETUP
   * Creates worker VM that will listen for messages (this could take a few minutes)
   * GET webserver/api/jobs/1/?  (stage settings)
   * Send job_stage message to queue
   * POST webserver/api/jobs/1 status=STAGING
3. worker receives job_stage message
   * downloads files and workflow
   * sends job_stage_complete
4. lando receives job_stage_complete
   * GET webserver/api/jobs/1/fields  (running settings)
   * Send run_workflow message to worker
   * POST webserver/api/jobs/1 status=RUNNING
5. worker receives run_workflow message
   * runs cwl-runner
   * Send run_workflow_complete message
6. lando receives run_workflow_complete message
   * GET webserver/api/jobs/1/?  (archive settings)
   * Send archive message to worker
   * POST webserver/api/jobs/1 status=ARCHIVING
7. worker receives archive message
   * saves data
   * Send archive_complete message
8. lando receives archive_complete message
   * POST webserver/api/jobs/1 status=COMPLETE
   * terminates worker VM
