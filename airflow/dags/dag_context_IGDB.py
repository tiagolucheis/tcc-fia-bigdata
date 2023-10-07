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
    'start_date': datetime(2023, 10, 7)
}

jars = '/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
        /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
        /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
        /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', '')

application = '/usr/local/airflow/dags/spark_scripts/create_context_IGDB.py'

dag = DAG(dag_id='dag_context_IGDB',
          default_args=default_args,
          schedule_interval='30 9 * * *',
          tags=['CONTEXT', 'IGDB', 'Delta']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_extraction = DailyExternalTaskSensor(
                    task_id = 'sensor_extraction',
                    external_dag_id = 'dag_raw_IGDB',
                    external_task_id = 'read_data_IGDB',
                    check_existence = False,
                    dag = dag
                    )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


# ------------------ Game Modes ------------------

specific_args_game_modes = {
    'api_name': 'igdb',
    'endpoint': 'game_modes',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

app_args_game_modes = [f"--{key}={value}" for key, value in specific_args_game_modes.items()]

task_game_modes = SparkSubmitOperator(
                            task_id='create_context_IGDB_game_modes',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=app_args_game_modes,
                            dag=dag
                        )

# ------------------ Genres ------------------

specific_args_genres = {
    'api_name': 'igdb',
    'endpoint': 'genres',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

app_args_genres = [f"--{key}={value}" for key, value in specific_args_genres.items()]

task_genres = SparkSubmitOperator(
                            task_id='create_context_IGDB_genres',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=app_args_genres,
                            dag=dag
                        )

# ------------------ Platforms ------------------

specific_args_platforms = {
    'api_name': 'igdb',
    'endpoint': 'platforms',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url summary'
}

app_args_platforms = [f"--{key}={value}" for key, value in specific_args_platforms.items()]

task_platforms = SparkSubmitOperator(
                          task_id='create_context_IGDB_platforms',
                          conn_id='spark_local',
                          jars=jars,
                          application=application,
                          application_args=app_args_platforms,
                          dag=dag
                      )

# ------------------ Player Perspectives ------------------

specific_args_platforms = {
    'api_name': 'igdb',
    'endpoint': 'player_perspectives',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'cheksum slug url'
}

app_args_platforms = [f"--{key}={value}" for key, value in specific_args_platforms.items()]

task_player_perspectives = SparkSubmitOperator(
                          task_id='create_context_IGDB_player_perspectives',
                          conn_id='spark_local',
                          jars=jars,
                          application=application,
                          application_args=app_args_platforms,
                          dag=dag
                      )




# Pipeline definition

start_dag >> sensor_extraction >> [task_game_modes, task_genres, task_platforms, task_player_perspectives] >> dag_finish