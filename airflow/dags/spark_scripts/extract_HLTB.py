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

from minio import Minio
import io

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
            StructField("IGDB_Game_Version", LongType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])

# Limite de registros a serem salvos por arquivo json
data_save_limit = 5000

# Número máximo de corrotinas simultâneas
max_concurrent_coroutines = 25

# Tamanho dos Chunks de dados a serem extraídos da API
chunk_size = 1000

# Define o path da tabela de controle de atualizações
control_table_path = bucket_path + '/control_table/'

# Define o path das listas de jogos a serem extraídos (IGDB e HLTB)
igdb_game_list_path = 's3a://raw/igdb/games/delta/'
hltb_game_list_path = 's3a://raw/hltb/delta/'


# Define a data e hora do início da extração para a tabela de controle de atualizações 
extraction_time = datetime.now() - timedelta(hours=3) #GMT -0300 (Horário Padrão de Brasília)

# Define a data de extração para particionamento no Lake
extraction_date = extraction_time.strftime("%Y-%m-%d")

# Define a data e hora do início da extração em formato timestamp
extraction_timestamp = int(extraction_time.timestamp())

# Início do Prcesso de Extração
start_time = time.time()


# Número de arquivos json a serem salvos
files = 0

# Lê a tabela de controle ou cria uma nova se ela não existir
try:
    df_control = spark.read.parquet(control_table_path)
    
except AnalysisException:
    df_control = spark.createDataFrame([], schema=schema)

# Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental)
if df_control.count() == 0:
    
    load_type = 'Initial'

    print("Step 1: Carregando a lista de jogos a serem extraídos...")

    # Carrega a lista de jogos a serem extraídos
    df_games = (
        DeltaTable
        .forPath(spark, igdb_game_list_path)
    )
    
    # Define a data e hora da última versão dos dados
    df_games_version = df_games.history().selectExpr("version", "timestamp").orderBy(fn.desc("version")).first()[0]

    df_games = (
        df_games
        .toDF()
        .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
        .select("id", "name", "release_year")
    )

    print("Step 2: Lista definida")

    # RETIRAR! Amostragem para testes 
    sample = 5000
    frac = sample / df_games.count()
    df_games = df_games.sample(withReplacement=False, fraction=frac, seed=42).limit(sample)

    # Adiciona uma coluna de numeração para facilitar a divisão
    df_games = df_games.withColumn("row_num", fn.monotonically_increasing_id())

    # Adiciona uma coluna com o número do chunk
    df_games = df_games.withColumn("chunk_num", fn.floor(df_games.row_num / chunk_size))

    # Computa o número total de chunks
    total_chunks = df_games.agg(fn.max("chunk_num")).collect()[0][0] + 1

    # Cria buffer para acumular os resultados extraídos da API
    json_buffer = []

    print("Step 3: Definição dos buffers concluída")

    # Cria uma fila para enfileirar os resultados
    queue = asyncio.Queue()

    # Configura as informações de acesso ao MinIO para listar os objetos
    minio_client = Minio("minio:9000", access_key="aulafia", secret_key="aulafia@123", secure=False)

    # Extrai e processa os dados por chunks
    for chunk in range(total_chunks):

        print("Chunk " + str(chunk + 1) + " de " + str(total_chunks) + ".")

        # Filtra os dados do chunk atual
        df_chunk = df_games.filter(fn.col("chunk_num") == chunk)

        # Inicia as tarefas de enfileiramento
        extracted_data = asyncio.run(loop_hltb("GN", spark, df_chunk.collect(), queue, max_concurrent_coroutines, extraction_timestamp))

        print("Quantidade de jogos encontrados: " + str(extracted_data.count()) + ".")

        # Acumula os resultados extraídos da API
        json_buffer.extend(extracted_data.toJSON().collect())

        # Verifica se o buffer atingiu o limite de registros a serem salvos por arquivo json
        if len(json_buffer) >= data_save_limit:

            files += 1

            # Remove os registros excedentes do buffer
            data_to_save = json_buffer[:data_save_limit]

            # Atualiza o buffer com os registros remanescentes
            json_buffer = json_buffer[data_save_limit:]

            # Nome do arquivo a ser salvo
            file_name = api_name + '/' + extraction_date + '/' + api_name + '_page_' + str(files).zfill(3) + '.json'

            # Transforma os dados em formato json
            json_data = '\n'.join(data_to_save)

            # Salva o arquivo json no bucket
            minio_client.put_object('landing-zone', file_name, io.BytesIO(json_data.encode('utf-8')), len(json_data))
            print("Arquivo salvo")
            
    # Verifica se o buffer ainda possui registros a serem salvos
    if len(json_buffer) > 0:
            
            files += 1

            # Nome do arquivo a ser salvo
            file_name = api_name + '/' + extraction_date + '/' + api_name + '_page_' + str(files).zfill(3) + '.json'

            # Transforma os dados em formato json
            json_data = '\n'.join(json_buffer)

            # Salva o arquivo json no bucket
            minio_client.put_object('landing-zone', file_name, io.BytesIO(json_data.encode('utf-8')), len(json_data))
            print("Arquivo salvo")

    print("Step 4: Extração dos chunks concluída")

    print(f"Foram importados {files} arquivos json para a Carga Inicial")

else:

    load_type = 'Incremental'

    # Carrega a lista de jogos a serem extraídos (com base na lista de jogos já extraídos do How Long To Beat)
    df_games_hltb = (
        DeltaTable
        .forPath(spark, hltb_game_list_path).toDF()
        .select("game_id", "id")
    )

    # Cria uma fila para enfileirar os resultados
    queue = asyncio.Queue()

    # Inicie as tarefas de enfileiramento
    extracted_data_hltb = asyncio.run(loop_hltb("ID", spark, df_games_hltb.collect(), queue, max_concurrent_coroutines, extraction_timestamp))

    # Carrega a lista de jogos a serem extraídos (com base na lista de jogos já extraídos do How Long To Beat)
    df_games_latest = (
        DeltaTable
        .forPath(spark, igdb_game_list_path)
    )

    # Define a data e hora da última versão dos dados
    df_games_version = df_games_latest.history().selectExpr("version", "timestamp").orderBy(fn.desc("version")).first()[0]

    df_games_latest = (
        df_games_latest
        .toDF()
        .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
        .select("id", "name", "release_year")
    )
    
    # Define a data e hora da última atualização dos dados
    previous_version = df_control.orderBy(fn.col("Exec_time"), asc=False).first()["IGDB_Game_Version"]    
    
    
    df_games_previous = (
        spark.read.format("delta")
        .option("versionAsOf", previous_version).load(igdb_game_list_path)
        .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
        .select("id", "name", "release_year")
    )
    
    delta_df = df_games_latest.exceptAll(df_games_previous)

    print("Número de jogos a serem extraídos: " + str(delta_df.count()) + ".")

    # Remove os registros que já se encontram na tabela HLTB
    df_games = delta_df.join(df_games_hltb, delta_df.id == df_games_hltb.id, how='left_anti').select("id", "name", "release_year")
    
    print ("Número de jogos a serem extraídos (removendo os já consultados): " + str(df_games.count()) + ".")

    # Inicia as tarefas de enfileiramento
    extracted_data_igdb = asyncio.run(loop_hltb("GN", spark, df_games.collect(), queue, max_concurrent_coroutines, extraction_timestamp))

    extracted_data = extracted_data_hltb.unionByName(extracted_data_igdb, allowMissingColumns = True)

    # Define o número de arquivos json a serem salvos
    files = math.ceil(extracted_data.count() / data_save_limit)
    
    print("Número de jogos extraídos: " + str(extracted_data.count()) + ".")
    
    # Salva os arquivos json no bucket
    for i in range(files):

        df_part = extracted_data.limit(data_save_limit)
        df_part.write.json(bucket_path + extraction_date + '/' + api_name + '_page_' + str(i+1).zfill(3) + '.json', mode='overwrite')
        extracted_data = extracted_data.subtract(df_part)

        print("Arquivo " + str(i+1) + " de " + str(files) + " - " + str(df_part.count()) + " registros.")

    print(f"Foram importados {files} arquivos json para a Carga Incremental")


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
                                    extraction_timestamp,
                                    load_type,
                                    files,
                                    df_games_version,
                                    formatted_time)], schema=schema)
# Salva o novo registro da tabela de controle 
(df_control
    .write
    .format('parquet')
    .mode('append')
    .save(control_table_path)
)