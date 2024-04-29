import sys
import boto3
import unittest

class EMRContainers:
    """
    An implementation of running a PySpark job on EMR Containers.

    This class provides support for 
    - finding EMR virtual cluster ID and jobs based on a tenant’s namespace
    - starting a job on the EMR virtual cluster
    - polling job status by job-id and virtual-cluster-id
    - canceling job
    

    By default, all calls are synchronous in that they wait for the Application to reach the desired state.
    - `run_spark_job` waits until the job is in a terminal state.
    """
    def __init__(self, REGION: str) -> None:
        self.client = boto3.client(
             "emr-containers", 
             region_name=REGION
        )
    def __str__(self):
        return f"EMR Containers Job: {self.job_id} Virtual Cluster: {self.virtual_cluster_id}" 
    
    """ Parameter value examples to start a Spark job:
        REGION = 'eu-west-2'
        NAME_SPACE = 'emr-karpenter'
        EMR_EKS_EXECUTION_ARN = 'arn'
        S3_BUCKET = 'bucket_name'
        JOB_NAME = 'emr-spark-python'
        LOG_GROUP = "/emr-on-eks-spark"
        LOG_STREAM_PREFIX = "spark_log_stream_prefix"
        DB_CONN_URL = "jdbc:mysql://MYS00629:3306/ndm_iceberg_metadata"
        USER_NAME = "ndm_iceberg_admin"
        PASSWORD = "ml6PJlu_G"
        EntryPoint = "s3://S3Bucket/simplehive.py"
        JARS = "s3://S3Bucket/mysql-connector-java-8.0.30.jar"
    """
    def start_spark_job(
        self, 
        NAME_SPACE: str,
        EMR_EKS_EXECUTION_ARN: str,
        S3_BUCKET: str,
        JOB_NAME: str,
        LOG_GROUP: str,
        LOG_STREAM_PREFIX: str,
        DB_CONN_URL: str,
        USER_NAME: str,
        PASSWORD: str,
        EntryPoint: str,
        JARS: str,
        Arguments: list(),
        Driver_cores: str,
        Executor_cores: str,
        Executor_memory: str,

        #wait: bool = True,
    ) -> str:
        EMR_EKS_CLUSTER_ID = self.get_cluster_by_ns(NAME_SPACE)

        response = self.client.start_job_run(
        name=JOB_NAME,
        virtualClusterId=EMR_EKS_CLUSTER_ID,
        executionRoleArn=EMR_EKS_EXECUTION_ARN,
        releaseLabel='emr-6.15.0-20231109',
        jobDriver={
          'sparkSubmitJobDriver': {
            "entryPoint": EntryPoint,
            "entryPointArguments": Arguments,
            "sparkSubmitParameters": "--jars " + JARS + " --conf spark.executor.memory=" + Executor_memory + " --conf spark.executor.cores=" + Executor_cores + " --conf spark.driver.cores=" + Driver_cores
          }
        },
        configurationOverrides={
        'monitoringConfiguration': {
            "cloudWatchMonitoringConfiguration": {
               "logGroupName": LOG_GROUP,
               "logStreamNamePrefix": LOG_STREAM_PREFIX
             },
             "s3MonitoringConfiguration": {
               "logUri": "s3://"+S3_BUCKET
             }
        
        },
        "applicationConfiguration":
        [
        {
         "classification": "emr-containers-defaults",
          "properties": {
             "job-start-timeout":"600"
          }
        },
        {
          "classification": "spark-defaults",
          "properties": {
            "spark.dynamicAllocation.enabled": "false",
            "spark.kubernetes.executor.deleteOnTermination": "true"
         }

        },
        {
          "classification": "spark-hive-site",
          "properties": {
	          "hive.metastore.client.factory.class": "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory",
	          "javax.jdo.option.ConnectionDriverName": "com.mysql.cj.jdbc.Driver",
            "javax.jdo.option.ConnectionUserName": USER_NAME,
            "javax.jdo.option.ConnectionPassword": PASSWORD,
            "javax.jdo.option.ConnectionURL": DB_CONN_URL
         }
        },
        {
          "classification": "spark-env",
          "properties": {
          },
          "configurations": [
          {
            "classification": "export",
            "properties": {
                "_JAVA_OPTIONS": "\"$_JAVA_OPTIONS -Dhttp.proxyHost=applicationwebproxy.nomura.com -Dhttp.proxyPort=80 -Dhttp.nonProxyHosts=s3.amazonaws.com\""
            }
          }
          ]
        }
        ]})
        job_id = response.get("id")
        return job_id
        """            "_JAVA_OPTIONS": "\"$_JAVA_OPTIONS -Dhttp.proxyHost=applicationwebproxy.nomura.com -Dhttp.proxyPort=80 -Dhttp.nonProxyHosts=s3.amazonaws.com\""
        """
        """         job_done = False
        while wait and not job_done:
            jr_response = self.get_job_run(job_id)
            job_done = jr_response.get("state") in [
                "COMPLETED",
                "FAILED",
                "CANCEL_PENDING",
                "CANCELLED",
            ] 
        return job_id
        """

    def describe_job(self, job_id: str, virtual_cluster_id: str) -> dict:
        response = self.client.describe_job_run(
            id=job_id, 
            virtualClusterId=virtual_cluster_id
        )
        return response.get("jobRun")
    
    def cancel_job(self, job_id: str, virtual_cluster_id: str) -> dict:
        response = self.client.cancel_job_run(
            id=job_id,
            irtualClusterId=virtual_cluster_id
        )
        return response.get("jobRun")
    
    """ Parameter value example for list_job_runs
    virtualClusterId='string',
    createdBefore=datetime(2015, 1, 1),
    createdAfter=datetime(2015, 1, 1),
    name='string',
    states=[
        'PENDING'|'SUBMITTED'|'RUNNING'|'FAILED'|'CANCELLED'|'CANCEL_PENDING'|'COMPLETED',
    ],
    maxResults=123,
    nextToken='string'
    """
    def list_jobs(self, 
            NAME_SPACE: str,
            created_before: str,
            created_after: str,
            #name: str,            
            states: list,
            #maxResults: int,
            #nextToken: str,
        ) -> dict:
        virtual_cluster_id = self.get_cluster_by_ns(NAME_SPACE)

        response = self.client.list_job_runs(
            virtualClusterId=virtual_cluster_id,
            createdBefore=created_before,
            createdAfter=created_after,
            #name=name,
            states=states,
            #maxResults=maxResults,
            #nextToken=nextToken
        )
        return response.get("jobRuns") 
    
    """ Parameter value example for list_virtual_clusters:
    containerProviderId='string',
    containerProviderType='EKS',
    createdAfter=datetime(2015, 1, 1),
    createdBefore=datetime(2015, 1, 1),
    states=[
        'RUNNING'|'TERMINATING'|'TERMINATED'|'ARRESTED',
    ],
    maxResults=123,
    nextToken='string'
    """    
    def list_clusters(self, 
            container_provider_type: str,
            states: list,
        ) -> dict:
        response = self.client.list_virtual_clusters(
            containerProviderType=container_provider_type,
            states=states,
        )
        return response.get("virtualClusters") 
    
    def get_cluster_by_ns (self, tenant_ns: str,) -> str:
        resp = self.list_clusters( 'EKS', ['RUNNING'])
        for vc in resp:
            namespace = vc['containerProvider'].get('info').get('eksInfo').get('namespace')
            if tenant_ns == namespace:
                return vc['id']

        return None

# python3 -m unittest emr_containers.MyTestClass.testStartAndDesc
class MyTestClass(unittest.TestCase):

    def testStartAndDesc(self):
        emr = EMRContainers('us-east-1')

        job_id = emr.start_spark_job("emr",
        "arn:aws:iam::339573364106:role/HiveEMRonEKS-sparkpermissionEMRJobExecRoleF94B9453-SKEn32xWEtxU",
        "hiveemroneks-appcode291f5ddb-udcvqqljbqz6",
        "hiveJdbcBoto3Test",
        "/emr-on-eks",
        "jdbc-hive",
        "jdbc:mysql://hiveemroneks-rdsaurora6c66f7da-ibwxobqw6rrd.cluster-csin4exixsgc.us-east-1.rds.amazonaws.com:3306/HiveEMRonEKS",
        "admin",
        "^GJ6agb4Cv,Ng8PQs_LgEJecoWmaO2",
        "s3://hiveemroneks-appcode291f5ddb-udcvqqljbqz6/app_code/job/hivejdbc1.py",
        "s3://hiveemroneks-appcode291f5ddb-udcvqqljbqz6/mysql-connector-java-8.0.28.jar",
        ['s3://hiveemroneks-appcode291f5ddb-udcvqqljbqz6'],
        "1",
        "1",
        "4G"
        )

        print("Started job = " + job_id)

        resp = emr.describe_job(job_id, "7jikhd68rwbq1bom3judznz13")
        print(resp)

    def testListJobs(self):
        emr = EMRContainers('us-east-1')

        resp = emr.list_jobs(
            "emr",
            "2024-1-19T00:00:00Z",
            "2023-12-31T00:00:00Z",
            ["RUNNING", "COMPLETED", "FAILED"],
        )
        print(resp)

    def testListClusters(self):
        emr = EMRContainers('us-east-1')

        resp = emr.list_clusters("EKS", ["RUNNING"])
        print(resp)

    def testGetClusterByNs(self):
        emr = EMRContainers('us-east-1')

        ns = "emr-eks-workshop-namespace"
        id = emr.get_cluster_by_ns(ns)
        print("Virtual-Cluster-Id " + id + " for namespace " + ns)

        ns1 = "emr"
        id1 = emr.get_cluster_by_ns(ns1)
        print("Virtual-Cluster-Id " + id1 + " for namespace " + ns1)

        ns2 = "non-exiting-emr"
        id2 = emr.get_cluster_by_ns(ns2)
        if id2 is None:
            print("Virtual-Cluster-Id for namespace " + ns2 + " does not exist")
        

if __name__ == '__main__':
    unittest.main()