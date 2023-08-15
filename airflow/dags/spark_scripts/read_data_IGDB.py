# Define os imports necessários para a execução do código
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
from minio import Minio
from delta.tables import DeltaTable
import os, time, json


# Define a sessão do Spark com os jars necessários para conexão com o MINIO
spark = (SparkSession.builder
         .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

# Configura as informações de acesso ao MinIO para listar os objetos
minio_client = Minio("minio:9000", access_key="aulafia", secret_key="aulafia@123", secure=False)


# Define o nome da API de onde foram extraídos os dados via arquivo JSON
api_name = 'igdb'


# Lista de Endpoints a serem lidos
endpoints = ["external_games"] # ["games", "genres", "game_modes", "player_perspectives", "platforms", "external_games"]

# Define o nome do bucket de origem dos dados extraídos
source_bucket_name = 'landing-zone'

# Define o nome do bucket de destino dos dados extraídos
target_bucket_name = 'raw'


# Define o path do bucket de origem dos dados
source_bucket_path = 's3a://' + source_bucket_name + '/' + api_name + '/'

# Define o path do bucket de destino dos dados
target_bucket_path = 's3a://' + target_bucket_name + '/' + api_name + '/'


# Define o schema da tabela de controle de atualizações
schema = StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Loaded_date", StringType(), nullable=False),
            StructField("Read_Rows", StringType(), nullable=False),
            StructField("Table_Rows", StringType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])

# Define a data e hora do início da leitura para a tabela de controle de atualizações 
read_time = datetime.now() - timedelta(hours=3) #GMT -0300 (Horário Padrão de Brasília)


# Executa um loop para varrer todos os endpoints

start_time = time.time()

for endpoint in endpoints:
    
    print("Iniciando tabela: " + endpoint) 

    start_time_endpoint = time.time()

    source_control_table_path = source_bucket_path + endpoint + '/control_table/'
    target_control_table_path = target_bucket_path + endpoint + '/control_table/'
    
    delta_table_path = target_bucket_path + endpoint + '/delta/'

    # Lê a tabela de controle da origem
    df_source_control = spark.read.parquet(source_control_table_path)
    
    # Lê a tabela de controle do destino ou cria uma nova se ela não existir
    try:
        df_target_control = spark.read.parquet(target_control_table_path)
        
    except AnalysisException:
        df_target_control = spark.createDataFrame([], schema=schema)
    
    # Verifica se a tabela de controle está vazia, criando um Dataframe vazio se for o caso, e identificando a última data executada
    if df_target_control.count() == 0:
        df_actual = None
    
        # Não há leitura prévia
        last_read = None

    # Caso contrário, carrega o Dataframe com os dados atuais
    else:
        df_actual = DeltaTable.forPath(spark, delta_table_path)
    
        # Define a data e hora da última leitura realizada
        last_read = df_target_control.select(fn.max(fn.col("Loaded_date")).alias("Latest_read")).collect()[0][0]
    
    # Filtra as datas de execução em que houve a extração de dados
    valid_executions = df_source_control.where(df_source_control["Loaded_files"] > 0)

    # Cria uma lista contendo as datas que serão lidas
    exec_dates = valid_executions \
                    .withColumn("Exec_date", fn.date_format(fn.col("Exec_time").cast("timestamp"), "yyyy-MM-dd")) \
                    .distinct().select("Exec_date").orderBy("Exec_date") \
                    .rdd.flatMap(lambda x: x).collect()

    if df_actual is not None:
        # Filtra as datas de execução que ainda não foram lidas
        exec_dates = [date for date in exec_dates if date > last_read]
        
    for date in exec_dates:
        
        print("Iniciando leitura de : " + date)
        
        start_time_date = time.time()
        
        # Lista de caminhos dos arquivos JSON no MinIO
        json_files = []

        # Lista os objeto no bucket e adiciona os caminhos dos arquivos à lista
        for obj in minio_client.list_objects(source_bucket_name, prefix=api_name + '/' + endpoint + '/' + date + "/"):
            path = f"s3a://{source_bucket_name}/{obj.object_name}"
            json_files.append(path)

        df_date = None
        
        # Loop para ler cada arquivo JSON e combinar os DataFrames
        for json_file in json_files:
            df_temp = spark.read.json(json_file)
            
            # Se o DataFrame inicial estiver vazio, atribui o DataFrame atual
            if df_date is None:
                df_date = df_temp
        
            # Caso contrário, combina o DataFrame atual com o DataFrame anterior
            else:
                df_date = df_date.unionByName(df_temp, allowMissingColumns=True)
        

        # Se o DataFrame inicial estiver vazio, atribui o DataFrame atual
        if (df_actual is None):
            df_actual = df_date
            df_actual_rows = df_actual.count()
        
            # Salva os dados como uma tabela Delta (overwrite sobrescreverá a tabela existente)
            df_actual.write.format("delta").mode("overwrite").save(delta_table_path)
            df_actual = DeltaTable.forPath(spark, delta_table_path)
        
        # Caso contrário, combina o DataFrame atual com o DataFrame anterior
        else:
            
            df_actual.alias("actualData") \
                .merge(source = df_date.alias("updatedData"),
                       condition = fn.expr("actualData.id = updatedData.id")) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            df_actual_rows = df_actual.toDF().count()

            # Salva o resultado do merge na tabela Delta (overwrite sobrescreverá a tabela existente)
            df_actual.toDF().write.format("delta").mode("overwrite").save(delta_table_path)

        end_time_date = time.time()
        execution_time_date = end_time_date - start_time_date

        # Calcula o tempo de execução
        hours, rem = divmod(execution_time_date, 3600)
        minutes, seconds = divmod(rem, 60)

        # Formata o tempo de execução
        formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
        df_target_control = spark.createDataFrame([(
                                            read_time.strftime("%Y-%m-%d %H:%M:%S"),
                                            int(read_time.timestamp()),
                                            date,
                                            df_date.count(),
                                            df_actual_rows,
                                            formatted_time)], schema=schema)
        # Salva o novo registro da tabela de controle 
        (df_target_control
            .write
            .format('parquet')
            .mode('append')
            .save(target_control_table_path)
            )

end_time = time.time()
execution_time = end_time - start_time

# Calcula o tempo de execução
hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)

# Formata o tempo de execução
formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

print(f"Leitura finalizada! Processo executado em {formatted_time}.")   