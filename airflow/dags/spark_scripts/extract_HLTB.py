import sys
sys.path.append('/usr/local/airflow/dags/scripts')

# Define os imports necessários para a execução do código
import pyspark.sql.functions as fn
import time, math, asyncio
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from HLTB_functions import loop_hltb


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

# Define o nome da API de onde serão extraídos os dados
api_name = 'hltb'

# Define o path do bucket de destino dos dados extraídos
bucket_path = 's3a://landing-zone/' + api_name + '/'

# Define o schema da tabela de controle de atualizações
schema = StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Load_type", StringType(), nullable=False),
            StructField("Loaded_files", LongType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])

# Limite de registros a serem salvos por arquivo json
data_save_limit = 1000

# Número máximo de corrotinas simultâneas
max_concurrent_coroutines = 25

control_table_path = bucket_path + '/control_table/'





    
# Define a data e hora do início da extração para a tabela de controle de atualizações 
extraction_time = datetime.now() - timedelta(hours=3) #GMT -0300 (Horário Padrão de Brasília)

# Define a data de extração para particionamento no Lake
extraction_date = extraction_time.strftime("%Y-%m-%d")

# Início do Prcesso de Extração
start_time = time.time()


# Lê a tabela de controle ou cria uma nova se ela não existir
try:
    df_control = spark.read.parquet(control_table_path)
    
except AnalysisException:
    df_control = spark.createDataFrame([], schema=schema)

# Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental)
if df_control.count() == 0:
    
    load_type = 'Initial'

    # Carrega a lista de jogos a serem extraídos
    game_list_path = 's3a://raw/igdb/games/delta/'
    df_games = (
        DeltaTable
        .forPath(spark, game_list_path).toDF()
        .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
        .select("id", "name", "release_year")
    )

    # RETIRAR! Amostragem para testes 
    sample = 100
    frac = sample / df_games.count()
    df_games = df_games.sample(withReplacement=False, fraction=frac, seed=42).limit(sample)

    # df_games = df_games.filter((fn.col('name') == "Frozen City") | (fn.col('name') == "God of War") | (fn.col('name') == "Tomb Raider"))


    # Crie uma fila para enfileirar os resultados
    queue = asyncio.Queue()

    # Inicie as tarefas de enfileiramento
    extracted_data = asyncio.run(loop_hltb(spark, df_games.collect(), queue, max_concurrent_coroutines))

    files = math.ceil(extracted_data.count() / data_save_limit)
    
    for i in range(files):

        df_part = extracted_data.limit(data_save_limit)
        df_part.write.json(bucket_path + extraction_date + '/' + api_name + '_page_' + str(i+1).zfill(3) + '.json', mode='overwrite')
        extracted_data = extracted_data.subtract(df_part)

        print("Arquivo " + str(i+1) + " de " + str(files) + " - " + str(df_part.count()) + " registros.")

    print(f"Foram importados {files} arquivos json para a Carga Inicial")

else:

    load_type = 'Incremental'

    print("Carga incremental não implementada para o How Long To Beat.")
    files = 0
    

end_time = time.time()
execution_time = end_time - start_time

# Calcula o tempo de execução
hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)

# Formata o tempo de execução
formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

print(f"Execução finalizada! Processo executado em {formatted_time}.")

# Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
df_control = spark.createDataFrame([(
                                    extraction_time.strftime("%Y-%m-%d %H:%M:%S"),
                                    int(extraction_time.timestamp()),
                                    load_type,
                                    files,
                                    formatted_time)], schema=schema)
# Salva o novo registro da tabela de controle 
(df_control
    .write
    .format('parquet')
    .mode('append')
    .save(control_table_path)
)