from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
print(BASE_DIR)
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from datetime import datetime
from airflow.sdk import task, dag
from shared.utils.commonUtils import download_file, delete_file
from shared.settings import URL

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DATA_DIR = BASE_DIR / "shared" / "data"
file_name = DATA_DIR / "BigMartSales.csv"


@task
def file_download(url, file_name):
    file = download_file(url=url, path=file_name)
    return file

@task
def file_delete(file_name):
    delete_file(file_name)
    return True

default_args={'owner':'pindio58',
              'depends_on_past':False,
              'retries':0,
              'email_on_failure':False}

@dag(default_args=default_args,
     catchup=False,
     dag_display_name='airflow-pyspark-integration',
     dag_id='airflow-pyspark-integration',
     schedule=None,
     start_date=datetime(2025,10,1))
def etl_run():
    get_file = file_download(URL,str(file_name))
    spark_submit = SparkSubmitOperator(
        task_id='run_etl_onariflow_for_spark',
        application='/opt/airflow/spark_jobs/etl.py',
        conn_id="spark-default",
        conf={
        "spark.jars": "file:///opt/spark/jars/hadoop-aws-3.3.4.jar,file:///opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        },
        verbose=True,
        name="airflow_spark_etl",
        env_vars={"JAVA_HOME":"/usr/lib/jvm/java-17-openjdk-arm64/"} 
    )

    del_fil = file_delete(file_name=get_file) 
    get_file >> spark_submit >> del_fil

etl_run()