# emr_containers
An implementation of running a PySpark job on EMR Containers

This class provides support for 
    - finding EMR virtual cluster ID and jobs based on a tenant’s namespace
    - starting a job on the EMR virtual cluster
    - polling job status by job-id and virtual-cluster-id
    - canceling job
    

    By default, all calls are synchronous in that they wait for the Application to reach the desired state.
    - `run_spark_job` waits until the job is in a terminal state.


