import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators.daily_task_sensor import DailyExternalTaskSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'aulafia',
    'start_date': datetime(2023, 10, 28)
}

jars = '/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
        /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
        /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
        /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', '')

application = '/usr/local/airflow/dags/spark_scripts/create_data_catalog.py'



# Pipeline definition

dag = DAG(dag_id='dag_data_catalog',
          default_args=default_args,
          schedule_interval='0 10 * * *',
          tags=['CONTEXT', 'APP', 'Hive']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_extraction_context_IGDB = DailyExternalTaskSensor(
                    task_id = 'sensor_extraction_context_IGDB',
                    external_dag_id = 'dag_context_IGDB',
                    external_task_id = 'dag_finish',
                    check_existence = False,
                    dag = dag
                    )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


task = SparkSubmitOperator(
                    task_id='create_data_catalog',
                    conn_id='spark_local',
                    jars=jars,
                    application=application,
                    dag=dag
                )



# Pipeline definition

start_dag >> sensor_extraction_context_IGDB >> task >> dag_finish