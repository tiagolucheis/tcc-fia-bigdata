# Define os imports necessários para a execução do código
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
from minio import Minio
from delta.tables import DeltaTable
import time
import concurrent.futures
from howlongtobeatpy import HowLongToBeat


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
# semaphore = asyncio.Semaphore(max_concurrent_coroutines)

# Define a data e hora do início da extração para a tabela de controle de atualizações 
extraction_time = datetime.now() - timedelta(hours=3) #GMT -0300 (Horário Padrão de Brasília)

# Define a data de extração para particionamento no Lake
extraction_date = extraction_time.strftime("%Y-%m-%d")

# Início do Prcesso de Extração
start_time = time.time()


control_table_path = bucket_path + '/control_table/'
    
# Lê a tabela de controle ou cria uma nova se ela não existir
try:
    df_control = spark.read.parquet(control_table_path)
    
except AnalysisException:
    df_control = spark.createDataFrame([], schema=schema)

# Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental) e a query a ser utilizada 
if df_control.count() == 0:
    load_type = 'Initial'
else:
    load_type = 'Incremental'

if load_type == 'Initial':

    # Carrega a lista de jogos a serem extraídos
    game_list_path = 's3a://raw/igdb/games/delta/'
    df_games = (
        DeltaTable
        .forPath(spark, game_list_path).toDF()
        .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
        .select("id", "name", "release_year")
    )

    # RETIRAR! Amostragem para testes 
    sample = 200
    frac = sample / df_games.count()
    df_games = df_games.sample(withReplacement=False, fraction=frac, seed=3).limit(sample)

    # df_games = df_games.filter((fn.col('name') == "Frozen City") | (fn.col('name') == "God of War") | (fn.col('name') == "Tomb Raider"))


    # Função para obter os dados do site How Long To Beat para um jogo
    def get_game_data(game):
        
        hltb = HowLongToBeat(0.01)  # Pequeno atraso entre as chamadas para não sobrecarregar a API
        results_list = hltb.search(game.name)
        
        best_element = None

        if results_list is not None and len(results_list) > 0:

            max_similarity = max(results_list, key=lambda element: element.similarity).similarity

            if (max_similarity > 0.8):
                
                # Filtrar elementos com a maior similaridade
                best_elements = [r for r in results_list if r.similarity == max_similarity]

                if (len(best_elements) > 1):
                    # Verifique o ano apenas nos elementos com a maior similaridade
                    for r in best_elements:
                        if r.release_world == game.release_year:
                            best_element = r
                            # print(f"Jogo: {game.name}, Ano: {game.release_year}, Resultado: {best_element.game_name}, Ano: {best_element.release_world}, Similaridade: {best_element.similarity}")
                            break  # Encontramos uma correspondência exata, então podemos sair do loop
                else:
                    best_element = best_elements[0]

        return game, best_element 

    # Função para processar os resultados
    def process_results(results):
        
        games_found = 0
        games_not_found = 0

        df_json = spark.createDataFrame([], StructType([]))
        file = 0

        for game, result in results:
            if result is None:
                #print(f"Jogo: {game.name}, Resultado: Não encontrado")
                games_not_found += 1
            else:
                #print(f"Jogo: {game.name}, Resultado: {result.game_name}")
                games_found += 1
            
                df = spark.read.json(spark.sparkContext.parallelize([result.json_content]))
                df = df.withColumn("igdb_id", fn.lit(game.id))

                df_json = df_json.unionByName(df, allowMissingColumns = True)

            if df_json.count() >= data_save_limit:

                file += 1
                
                # Salva os dados em formato JSON no MinIO
                df_json.write.json(bucket_path + extraction_date + '/' + api_name + '_page_' + str(file).zfill(3) + '.json', mode='overwrite')
                df_json = None

        if df_json is not None and df_json.count() > 0:
            file += 1
            
            # Salva os dados em formato JSON no MinIO
            df_json.write.json(bucket_path + extraction_date + '/' + api_name + '_page_' + str(file).zfill(3) + '.json', mode='overwrite')
            df_json = None

        print(f"Jogos encontrados: {games_found}")
        print(f"Jogos não encontrados: {games_not_found}")

        print(f"Foram importados {file} arquivos json para a Carga Inicial")

        return file

    # Função para executar o loop de extração dos dados de cada jogo, de forma assíncrona e concorrente
    def loop_hltb():
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_coroutines) as executor:
            resultados = list(executor.map(get_game_data, df_games.collect()))
        
        files = process_results(resultados)
        
        return files
    
    # Executa o loop de extração dos dados de cada jogo, de forma assíncrona e concorrente
    files = loop_hltb()
    
else:
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