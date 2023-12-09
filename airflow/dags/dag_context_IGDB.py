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


# Convertendo o dicionário em uma lista de argumentos
def get_application_args(specific_args):
    return [f"--{key}={value}" for key, value in specific_args.items()]



# Pipeline definition

dag = DAG(dag_id='dag_03_context_IGDB',
          default_args=default_args,
          schedule_interval='30 9 * * *',
          tags=['CONTEXT', 'IGDB', 'Delta']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_extraction_IGDB = DailyExternalTaskSensor(
                    task_id = 'sensor_extraction_IGDB',
                    external_dag_id = 'dag_02_raw_IGDB',
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

task_game_modes = SparkSubmitOperator(
                            task_id='create_context_IGDB_game_modes',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_game_modes),
                            dag=dag
                        )

# ------------------ Genres ------------------

specific_args_genres = {
    'api_name': 'igdb',
    'endpoint': 'genres',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_genres = SparkSubmitOperator(
                            task_id='create_context_IGDB_genres',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_genres),
                            dag=dag
                        )

# ------------------ Player Perspectives ------------------

specific_args_player_perspectives = {
    'api_name': 'igdb',
    'endpoint': 'player_perspectives',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'cheksum slug url'
}

task_player_perspectives = SparkSubmitOperator(
                          task_id='create_context_IGDB_player_perspectives',
                          conn_id='spark_local',
                          jars=jars,
                          application=application,
                          application_args=get_application_args(specific_args_player_perspectives),
                          dag=dag
                      )


# ------------------ Platforms ------------------

specific_args_platforms = {
    'api_name': 'igdb',
    'endpoint': 'platforms',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url summary versions websites'
}

task_platforms = SparkSubmitOperator(
                          task_id='create_context_IGDB_platforms',
                          conn_id='spark_local',
                          jars=jars,
                          application=application,
                          application_args=get_application_args(specific_args_platforms),
                          dag=dag
                      )



# ------------------ Keywords ------------------

specific_args_keywords = {
    'api_name': 'igdb',
    'endpoint': 'keywords',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'cheksum slug url'
}

task_keywords = SparkSubmitOperator(
                          task_id='create_context_IGDB_keywords',
                          conn_id='spark_local',
                          jars=jars,
                          application=application,
                          application_args=get_application_args(specific_args_keywords),
                          dag=dag
                      )



# ------------------ Language Supports ------------------

specific_args_language_supports = {
    'api_name': 'igdb',
    'endpoint': 'language_supports',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum'
}

task_language_supports = SparkSubmitOperator(
                            task_id='create_context_IGDB_language_supports',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_language_supports),
                            dag=dag
                        )



# ------------------ Themes ------------------

specific_args_themes = {
    'api_name': 'igdb',
    'endpoint': 'themes',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_themes = SparkSubmitOperator(
                            task_id='create_context_IGDB_themes',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_themes),
                            dag=dag
                        ) 



# ------------------ Multiplayer Modes ------------------

specific_args_multiplayer_modes = {
    'api_name': 'igdb',
    'endpoint': 'multiplayer_modes',
    'date_cols': '',
    'cols_to_drop': 'checksum'
}

task_multiplayer_modes = SparkSubmitOperator(
                            task_id='create_context_IGDB_multiplayer_modes',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_multiplayer_modes),
                            dag=dag
                        )



# ------------------ Franchises ------------------

specific_args_franchises = {
    'api_name': 'igdb',
    'endpoint': 'franchises',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_franchises = SparkSubmitOperator(
                            task_id='create_context_IGDB_franchises',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_franchises),
                            dag=dag
                        )



# ------------------ Collections ------------------

specific_args_collections = {
    'api_name': 'igdb',
    'endpoint': 'collections',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_collections = SparkSubmitOperator(
                            task_id='create_context_IGDB_collections',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_collections),
                            dag=dag
                        )



# ------------------ Game Engines ------------------

specific_args_game_engines = {
    'api_name': 'igdb',
    'endpoint': 'game_engines',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_game_engines = SparkSubmitOperator(
                            task_id='create_context_IGDB_game_engines',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_game_engines),
                            dag=dag
                        )



# ------------------ Companies ------------------

specific_args_companies = {
    'api_name': 'igdb',
    'endpoint': 'companies',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum slug url'
}

task_companies = SparkSubmitOperator(
                            task_id='create_context_IGDB_companies',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_companies),
                            dag=dag
                        )




specific_args_involved_companies = {
    'api_name': 'igdb',
    'endpoint': 'involved_companies',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum'
}

task_involved_companies = SparkSubmitOperator(
                            task_id='create_context_IGDB_involved_companies',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_involved_companies),
                            dag=dag
                        )


# ------------------ Websites ------------------

specific_args_websites = {
    'api_name': 'igdb',
    'endpoint': 'websites',
    'date_cols': '',
    'cols_to_drop': 'checksum'
}

task_websites = SparkSubmitOperator(
                            task_id='create_context_IGDB_websites',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_websites),
                            dag=dag
                        )



# ------------------ Age Ratings ------------------

specific_args_age_ratings = {
    'api_name': 'igdb',
    'endpoint': 'age_ratings',
    'date_cols': '',
    'cols_to_drop': 'checksum'
}

task_age_ratings = SparkSubmitOperator(
                            task_id='create_context_IGDB_age_ratings',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_age_ratings),
                            dag=dag
                        )



# ------------------ Age Rating Content Descriptions ------------------

specific_args_age_rating_content_descriptions = {
    'api_name': 'igdb',
    'endpoint': 'age_rating_content_descriptions',
    'date_cols': '',
    'cols_to_drop': 'checksum'
}

task_age_rating_content_descriptions = SparkSubmitOperator(
                            task_id='create_context_IGDB_age_rating_content_descriptions', 
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_age_rating_content_descriptions),
                            dag=dag
                        )



# ------------------ Covers ------------------

specific_args_covers = {
    'api_name': 'igdb',
    'endpoint': 'covers',
    'date_cols': '',
    'cols_to_drop': 'alpha_channel animated checksum height image_id width'
}

task_covers = SparkSubmitOperator(
                            task_id='create_context_IGDB_covers',  
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_covers),
                            dag=dag
                        )


# ------------------ Game Localizations ------------------

specific_args_game_localizations = {
    'api_name': 'igdb',
    'endpoint': 'game_localizations',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum'
}

task_game_localizations = SparkSubmitOperator(
                            task_id='create_context_IGDB_game_localizations',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_game_localizations),
                            dag=dag
                        )


# ------------------ Release Dates ------------------

specific_args_release_dates = {
    'api_name': 'igdb',
    'endpoint': 'release_dates',
    'date_cols': 'created_at updated_at date',
    'cols_to_drop': 'checksum'
}

task_release_dates = SparkSubmitOperator(
                            task_id='create_context_IGDB_release_dates',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_release_dates),
                            dag=dag
                        )



# ------------------ Games ------------------

specific_args_games = {
    'api_name': 'igdb',
    'endpoint': 'games',
    'date_cols': 'created_at first_release_date updated_at',
    'cols_to_drop': 'checksum slug videos alternative_names artworks screenshots tags'
}

task_games = SparkSubmitOperator(
                            task_id='create_context_IGDB_games',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_games),
                            dag=dag
                        )



# ------------------ External Games ------------------

specific_args_external_games = {
    'api_name': 'igdb',
    'endpoint': 'external_games',
    'date_cols': 'created_at updated_at',
    'cols_to_drop': 'checksum'
}

task_external_games = SparkSubmitOperator(
                            task_id='create_context_IGDB_external_games',
                            conn_id='spark_local',
                            jars=jars,
                            application=application,
                            application_args=get_application_args(specific_args_external_games),
                            dag=dag
                        )



# Pipeline definition

start_dag >> sensor_extraction_IGDB >> [task_game_modes, task_genres, task_platforms, task_player_perspectives, task_language_supports, task_keywords, task_themes, task_multiplayer_modes, task_franchises, task_collections, task_game_engines, task_companies, task_involved_companies, task_websites, task_age_ratings, task_age_rating_content_descriptions, task_covers, task_game_localizations, task_release_dates, task_external_games] >> task_games >> dag_finish