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
    'start_date': datetime(2023, 11, 30)
}

# Define as configurações de cada Sensor
sensor_configurations = {
    'sensor_context_IGDB': {
        'task_id': 'sensor_context_IGDB',
        'external_dag_id': 'dag_03_context_IGDB',
        'external_task_id': 'dag_finish'
    },
    'sensor_context_HLTB': {
        'task_id': 'sensor_context_HLTB',
        'external_dag_id': 'dag_03_context_HLTB',
        'external_task_id': 'dag_finish'
    }
}

# Define as configurações de cada DAG
dag_configurations = {
    'dag_04_trust_daily': {
        'dag_id': 'dag_04_trust_daily',
        'desired_days_of_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
        'scheduled_hour': 10,
        'scheduled_minute': 0
    },
    'dag_04_trust_weekly': {
        'dag_id': 'dag_04_trust_weekly',
        'desired_days_of_week': ['Sunday'],
        'scheduled_hour': 13,
        'scheduled_minute': 0
    }
}

jars = '/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
        /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
        /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
        /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', '')

application = '/usr/local/airflow/dags/spark_scripts/create_trust.py'


# Convertendo o dicionário em uma lista de argumentos
def get_application_args(specific_args):
    return [f"--{key}={value}" for key, value in specific_args.items()]



# Define o intervalo de execução da DAG de acordo com os dias da semana
def get_schedule_interval(hour, minute, desired_days_of_week):

    # Define o número do dia da semana de acordo com o calendário do cron
    days_of_week = {
        'Sunday': 0,
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6
    }

    # Converte o número do dia da semana para o formato do cron
    desired_days_of_week = [str(days_of_week[day]) for day in desired_days_of_week]

    # Retorna o intervalo de execução da DAG
    return f"{minute} {hour} * * {','.join(desired_days_of_week)}"



# Gera as DAGs de acordo com suas configurações e as configurações dos sensores 
def generate_dag(dag_config, default_args, sensors_config, tasks_config):

    # Definições da DAG
    dag = DAG(
        dag_id=dag_config['dag_id'],
        default_args=default_args,
        schedule_interval=get_schedule_interval(dag_config['scheduled_hour'], dag_config['scheduled_minute'], dag_config['desired_days_of_week']),
        tags=['TRUST', 'IGDB', 'HLTB', 'Delta']
    )

    # Cria a task de início da DAG
    start_dag = DummyOperator(task_id='start_dag', dag=dag)

    # Cria os sensores de execução das DAGs externas, com base na lista de sensores recebida    
    sensors = [DailyExternalTaskSensor(
                    task_id = sensor['task_id'],
                    external_dag_id = sensor['external_dag_id'],
                    external_task_id = sensor['external_task_id'],
                    check_existence = False,
                    dag = dag
                    ) for sensor in sensors_config]
    
    # Define a parte inicial do pipeline de execução
    start_dag >> sensors
    previous_task = sensors

    # Cria a lista de tasks, definindo as dependências e o paralelismo
    for task_list in tasks_config:

        # Cria as tasks de acordo com as configurações de cada task, em paralelo
        current_task_list = [SparkSubmitOperator(
                                task_id=task['task_id'],
                                conn_id='spark_local',
                                jars=jars,
                                application=application,
                                application_args=get_application_args(task['specific_args']),
                                dag=dag,
                                ) for task in task_list]
        
        # Adiciona as tasks ao pipeline
        for task in current_task_list:
                previous_task >> task

        previous_task = current_task_list

        # Define a dependência entre as listas de tasks no pipeline de execução
        # chain(*previous_task, *current_task_list)
        # previous_task = current_task_list
    
    # Cria a task de conclusão da DAG
    dag_finish = DummyOperator(task_id='dag_finish', dag=dag)

    # Define a parte final do pipeline de execução
    previous_task >> dag_finish

    return dag
    


# Define as configurações de cada task
task_configurations = {
    'task_games_by_platform': {
        'task_id': 'create_games_by_platform',
        'specific_args': {
            'table_name': 'games_by_platform',
            'source_tables': 'igdb.games igdb.platforms igdb.release_dates',
        }
    },
    'task_games_age_ratings': {
        'task_id': 'create_games_age_ratings',
        'specific_args': {
            'table_name': 'games_age_ratings',
            'source_tables': 'igdb.games igdb.age_ratings',
        }
    },
    'task_game_modes_by_game': {
        'task_id': 'create_game_modes_by_game',
        'specific_args': {
            'table_name': 'game_modes_by_game',
            'source_tables': 'igdb.games igdb.game_modes',
        }
    },
    'task_games_by_franchise': {
        'task_id': 'create_games_by_franchise',
        'specific_args': {
            'table_name': 'games_by_franchise',
            'source_tables': 'igdb.games igdb.franchises',
        }
    },
    'task_games_by_genre': {
        'task_id': 'create_games_by_genre',
        'specific_args': {
            'table_name': 'games_by_genre',
            'source_tables': 'igdb.games igdb.genres',
        }
    },
    'task_games_by_keyword': {
        'task_id': 'create_games_by_keyword',
        'specific_args': {
            'table_name': 'games_by_keyword',
            'source_tables': 'igdb.games igdb.keywords',
        }
    },
    'task_games_by_themes': {
        'task_id': 'create_games_by_themes',
        'specific_args': {
            'table_name': 'games_by_themes',
            'source_tables': 'igdb.games igdb.themes',
        }
    },
    'task_language_supports_by_game': {
        'task_id': 'create_language_supports_by_game',
        'specific_args': {
            'table_name': 'language_supports_by_game',
            'source_tables': 'igdb.games igdb.language_supports',
        }
    },
    'task_player_perspectives_by_game': {
        'task_id': 'create_player_perspectives_by_game',
        'specific_args': {
            'table_name': 'player_perspectives_by_game',
            'source_tables': 'igdb.games igdb.player_perspectives',
        }
    },
    'task_involved_companies_by_game': {
        'task_id': 'create_involved_companies_by_game',
        'specific_args': {
            'table_name': 'involved_companies_by_game',
            'source_tables': 'igdb.involved_companies igdb.companies igdb.games',
        }
    },
    'task_games': {
        'task_id': 'create_games',
        'specific_args': {
            'table_name': 'games',
            'source_tables': 'igdb.games hltb.games igdb.covers',
        }
    },
    'task_indie_games': {
        'task_id': 'create_indie_games',
        'specific_args': {
            'table_name': 'indie_games',
            'source_tables': 'trust.games_by_genre trust.games trust.games_by_platform trust.involved_companies_by_game',
        }
    }
}

# Define a lista de tasks e suas dependências
tasks = [
    [task_configurations['task_games_by_platform'], task_configurations['task_games_age_ratings'], task_configurations['task_game_modes_by_game'], task_configurations['task_games_by_franchise'], task_configurations['task_games_by_genre'], task_configurations['task_games_by_keyword'], task_configurations['task_games_by_themes'], task_configurations['task_language_supports_by_game'], task_configurations['task_player_perspectives_by_game'], task_configurations['task_involved_companies_by_game']],
    [task_configurations['task_games']],
    [task_configurations['task_indie_games']]
]



# Criação das DAGs
dag_daily = generate_dag(
    dag_config=dag_configurations['dag_04_trust_daily'],
    default_args=default_args,
    sensors_config=[sensor_configurations['sensor_context_IGDB']],
    tasks_config=tasks
)

dag_weekly = generate_dag(
    dag_config=dag_configurations['dag_04_trust_weekly'],
    default_args=default_args,
    sensors_config=[sensor_configurations['sensor_context_IGDB'], sensor_configurations['sensor_context_HLTB']],
    tasks_config=tasks
)