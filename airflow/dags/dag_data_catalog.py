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

# Define as configurações de cada DAG
dag_configurations = {
    'dag_05_data_catalog_daily': {
        'dag_id': 'dag_05_data_catalog_daily',
        'desired_days_of_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
        'scheduled_hour': 10,
        'scheduled_minute': 15,
        'external_dag_id': 'dag_04_trust_daily'
    },
    'dag_05_data_catalog_weekly': {
        'dag_id': 'dag_05_data_catalog_weekly',
        'desired_days_of_week': ['Sunday'],
        'scheduled_hour': 13,
        'scheduled_minute': 15,
        'external_dag_id': 'dag_04_trust_weekly'
    }
}

jars = '/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
        /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
        /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
        /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
        /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', '')

application = '/usr/local/airflow/dags/spark_scripts/create_data_catalog.py'


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
def generate_dag(dag_config, default_args):

    # Definições da DAG
    dag = DAG(
        dag_id=dag_config['dag_id'],
        default_args=default_args,
        schedule_interval=get_schedule_interval(dag_config['scheduled_hour'], dag_config['scheduled_minute'], dag_config['desired_days_of_week']),
        tags=['LANDING', 'RAW', 'CONTEXT', 'TRUST', 'Hive']
    )

    # Cria a task de início da DAG
    start_dag = DummyOperator(task_id='start_dag', dag=dag)

    # Cria o sensore de execução da DAG externa    
    sensor = DailyExternalTaskSensor(
                task_id = 'sensor_trust_creation',
                external_dag_id = dag_config['external_dag_id'],
                external_task_id = 'dag_finish',
                check_existence = False,
                dag = dag
                )
    
    task = SparkSubmitOperator(
                    task_id='create_data_catalog',
                    conn_id='spark_local',
                    jars=jars,
                    application=application,
                    dag=dag
                )

    
    # Cria a task de conclusão da DAG
    dag_finish = DummyOperator(task_id='dag_finish', dag=dag)

    # Pipeline definition

    start_dag >> sensor >> task >> dag_finish

    return dag


# Criação das DAGs
dag_daily = generate_dag(
    dag_config=dag_configurations['dag_05_data_catalog_daily'],
    default_args=default_args
)

dag_weekly = generate_dag(
    dag_config=dag_configurations['dag_05_data_catalog_weekly'],
    default_args=default_args
)