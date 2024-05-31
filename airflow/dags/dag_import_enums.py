import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'aulafia',
    'start_date': datetime(2024, 5, 18)
}



# Pipeline definition

dag = DAG(dag_id='dag_01_import_enums',
          default_args=default_args,
          schedule_interval='0 5 * * *',
          tags=['LANDING', 'CSV']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

task_import_IGDB = SparkSubmitOperator(
                          task_id='import_IGDB_Enums',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/import_IGDB_Enums.py',
                          dag=dag
                      )

task_import_Aux = SparkSubmitOperator(
                          task_id='import_ISO_Enums',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/import_Aux_Enums.py',
                          dag=dag
                      )



dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


# Pipeline definition

start_dag >> [task_import_IGDB, task_import_Aux] >> dag_finish