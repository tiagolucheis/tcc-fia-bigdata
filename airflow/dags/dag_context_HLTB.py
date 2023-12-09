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
    'start_date': datetime(2023, 11, 26)
}

jars = '/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
        /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
        /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
        /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', '')

application = '/usr/local/airflow/dags/spark_scripts/create_context_HLTB.py'


# Convertendo o dicionário em uma lista de argumentos
def get_application_args(specific_args):
    return [f"--{key}={value}" for key, value in specific_args.items()]



# Pipeline definition

dag = DAG(dag_id='dag_03_context_HLTB',
          default_args=default_args,
          schedule_interval='30 12 * * 0',
          tags=['CONTEXT', 'HLTB', 'Delta']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_extraction_HLTB = DailyExternalTaskSensor(
                    task_id = 'sensor_extraction_HLTB',
                    external_dag_id = 'dag_02_raw_HLTB',
                    external_task_id = 'read_data_HLTB',
                    check_existence = False,
                    dag = dag
                    )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )



# ------------------ Games ------------------

specific_args_games = {
    'api_name': 'hltb',
    'endpoint': 'games',
    'date_cols': 'extracted_datetime',
    'cols_to_drop': 'game_image invested_co invested_co_count invested_mp invested_mp_count comp_lvl_co comp_lvl_combine comp_lvl_mp comp_lvl_sp comp_lvl_spd game_name_date profile_popular'


}

task_games = SparkSubmitOperator(
                            task_id='create_context_HLTB_games',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_games),
                            dag=dag
                        )



# Pipeline definition

start_dag >> sensor_extraction_HLTB >> task_games >> dag_finish