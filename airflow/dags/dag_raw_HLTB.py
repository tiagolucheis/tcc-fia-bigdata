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
    'start_date': datetime(2023, 9, 9)
}

# Argumentos específicos da DAG
specific_args = {
    'api_name': 'hltb',
    'endpoints': "games",
    'mod_date_col': "extracted_datetime"
}

# Converte o dicionário em uma lista de argumentos
application_args = [f"--{key}={value}" for key, value in specific_args.items()]


dag = DAG(dag_id='dag_02_raw_HLTB',
          default_args=default_args,
          schedule_interval='0 12 * * 0',
          tags=['RAW', 'HLTB', 'API', 'Delta']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_extraction = DailyExternalTaskSensor(
                    task_id = 'sensor_extraction',
                    external_dag_id = 'dag_01_extract_HLTB',
                    external_task_id = 'extract_HLTB',
                    check_existence = False,
                    dag = dag
                    )

task = SparkSubmitOperator(
                          task_id='read_data_HLTB',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/read_data.py',
                          application_args=application_args,
                          dag=dag
                      )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


start_dag >> sensor_extraction >> task >> dag_finish