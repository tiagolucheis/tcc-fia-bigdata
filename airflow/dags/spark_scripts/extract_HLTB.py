# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pyspark.sql.functions as fn
from datetime import datetime, timedelta, timezone
from minio import Minio
from howlongtobeatpy import HowLongToBeat
import time, aiohttp, asyncio, io, json


# Define a sessão do Spark com os jars necessários para conexão com o MINIO
def create_spark_session():
    return (SparkSession.builder
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



# Define o cliente do Minio
def get_minio_client():
    return Minio("minio:9000", access_key="aulafia", secret_key="aulafia@123", secure=False)



# Define as variáveis parametrizáveis do script
def get_configuration():
    configuration = {
        "api_name": 'hltb',
        
        "data_save_limit": 5000,            # Número máximo de registros a serem salvos por arquivo json
        "max_concurrent_coroutines": 25,    # Número máximo de corotinas a serem executadas simultaneamente
        "chunk_size": 150,                  # Número de registros por chunk
        "similarity_threshold": 0.8,        # Similaridade mínima para considerar um jogo como correspondente

        "bucket_path": 's3a://landing-zone/hltb/',
        "control_table_path": 's3a://landing-zone/hltb/games/control_table/',
        "igdb_game_list_path": 's3a://raw/igdb/games/delta/',
        "hltb_game_list_path": 's3a://raw/hltb/games/delta/',

        "schema": StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Load_type", StringType(), nullable=False),
            StructField("Loaded_files", LongType(), nullable=False),
            StructField("IGDB_Game_Version", LongType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])
    }
    return configuration



# Obtém a tabela de controle de atualizações do endpoint
def get_control_table(configuration, spark):
    try:
        return spark.read.parquet(configuration["control_table_path"])
    except AnalysisException:
        return spark.createDataFrame([], schema=configuration["schema"])



# Carrega a lista de jogos a serem extraídos (e sua versão) do Delta Lake do IGDB, já separando-os em chunks
def load_game_list(load_type, game_list_path, spark, chunk_size, api_name="", df_control=None):
    
    print("Step 1: Carregando a lista de jogos a serem extraídos...")

    df_games = (
            DeltaTable
            .forPath(spark, game_list_path)
    )

    if load_type == 'Initial':

        # Define a data e hora da última versão dos dados
        df_games_version = df_games.history().selectExpr("version", "timestamp").orderBy(fn.desc("version")).first()[0]

        df_games = (
            df_games
            .toDF()
            .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
            .select("id", "name", "release_year")
        )

    else:
        
        if api_name == 'HLTB':
            
            df_games_version = None

            df_games = (
                df_games
                .toDF()
                .select("game_id", "id")
            )  
            
        elif api_name == 'IGDB':

            # Define a data e hora da última versão dos dados
            df_games_version = df_games.history().selectExpr("version", "timestamp").orderBy(fn.desc("version")).first()[0]

            df_games = (
                df_games
                .toDF()
                .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
                .select("id", "name", "release_year")
            )

            # Define a data e hora da última atualização dos dados
            previous_version = df_control.orderBy(fn.col("Exec_time"), asc=False).first()["IGDB_Game_Version"]    
            
            df_games_previous = (
                spark.read.format("delta")
                .option("versionAsOf", previous_version).load(game_list_path)
                .withColumn("release_year", fn.year(fn.to_timestamp(fn.from_unixtime("first_release_date"))))
                .select("id", "name", "release_year")
            )

            df_games = df_games.exceptAll(df_games_previous)

            print("Número de jogos a serem extraídos: " + str(df_games.count()) + ".")

    # RETIRAR! Amostragem para testes 
    #sample = 1000
    #if df_games.count() > 0 and df_games.count() > sample:
        #frac = sample / df_games.count()
        #df_games = df_games.sample(withReplacement=False, fraction=frac, seed=42).limit(sample)

    # Adiciona uma coluna de numeração para facilitar a divisão
    df_games = df_games.withColumn("row_num", fn.monotonically_increasing_id())

    # Adiciona uma coluna com o número do chunk
    df_games = df_games.withColumn("chunk_num", fn.floor(df_games.row_num / chunk_size))

    print("Step 2: Carregando a lista de jogos a serem extraídos... OK")

    return df_games, df_games_version



# Obtém um chunk de dados da lista de jogos a serem extraídos
def get_data_chunk(df_games, chunk):
    return df_games.filter(fn.col("chunk_num") == chunk).drop("row_num", "chunk_num")



# Obtém os dados do site How Long To Beat para um jogo, a partir de seu nome
async def get_hltb_data_by_name(game, queue, semaphore, similarity_threshold):
    
    async with semaphore: 
        hltb = HowLongToBeat(0.01)  # Pequeno atraso entre as chamadas para não sobrecarregar a API
        
        results_list = await hltb.async_search(game['name'])
        
        best_element = None

        if results_list is not None and len(results_list) > 0:

            max_similarity = max(results_list, key=lambda element: element.similarity).similarity

            if (max_similarity > similarity_threshold):
                
                # Filtrar elementos com a maior similaridade
                best_elements = [r for r in results_list if r.similarity == max_similarity]

                if (len(best_elements) > 1):
                    # Verifique o ano apenas nos elementos com a maior similaridade
                    for r in best_elements:
                        if r.release_world == game['release_year']:
                            best_element = r
                            # print(f"Jogo: {game.name}, Ano: {game.release_year}, Resultado: {best_element.game_name}, Ano: {best_element.release_world}, Similaridade: {best_element.similarity}")
                            break  # Encontramos uma correspondência exata, então podemos sair do loop
                else:
                    best_element = best_elements[0]

        await queue.put((game, best_element))

        return game, best_element 



# Obtém os dados do site How Long To Beat para um jogo, a partir de seu ID
async def get_hltb_data_by_id(game, queue, semaphore):
    
    async with semaphore: 
        hltb = HowLongToBeat(0.01)  # Pequeno atraso entre as chamadas para não sobrecarregar a API
        
        best_element = await hltb.async_search_from_id(game['game_id'])
        await queue.put((game, best_element))

        return game, best_element 
    


# Processa os resultados extraídos da API
async def process_results(spark, queue, extraction_timestamp):
    
    df_json = spark.createDataFrame([], StructType([]))

    while not queue.empty():
        game, result = await queue.get()
        
        if result:
            df = result.json_content
            df['id'] = game['id']
            df['extracted_datetime'] = extraction_timestamp         

            try:
                df_json = df_json.unionByName(spark.createDataFrame([df]), allowMissingColumns = True)
            except:
                print(f"Erro ao processar o jogo {game['id']}.")
                print(f"Tipo do dado: {type(df)}")
                print(f"Conteúdo do dado: {df}")
                print("")
                print(f"Tipo do dado: {type(df_json)}")
                print(f"Conteúdo do dado: ")
                df_json.show(truncate=False)
                              
    return df_json



# Extrai os dados de um chunk de jogos, de forma assíncrona e concorrente
async def extract_data_chunk(search_type, spark, df_chunk, queue, max_concurrent_coroutines, extraction_timestamp, similarity_threshold):
    
    semaphore = asyncio.Semaphore(max_concurrent_coroutines)

    async with aiohttp.ClientSession() as session:
        
        if search_type == 'ID':
            tasks = [get_hltb_data_by_id(game.asDict(), queue, semaphore) for game in df_chunk.collect()]
        elif search_type == 'GN':
            tasks = [get_hltb_data_by_name(game.asDict(), queue, semaphore, similarity_threshold) for game in df_chunk.collect()]
        
        await asyncio.gather(*tasks)
        extracted_data = await process_results(spark, queue, extraction_timestamp)
    
    session.close()
    
    return extracted_data



# Salva o buffer em um arquivo JSON no Lake
def save_data_buffer(api_name, extraction_date, minio_client, data_buffer, files):
    
    # Nome do arquivo a ser salvo
    file_name = api_name + '/games/' + extraction_date + '/' + api_name + '_games_page_' + str(files).zfill(3) + '.json'

    # Transforma os dados em formato json
    json_data = json.dumps(data_buffer)

    # Salva o arquivo json no bucket
    minio_client.put_object('landing-zone', file_name, io.BytesIO(json_data.encode('utf-8')), len(json_data))



# Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
def update_control_table(configuration, spark, extraction_time, load_type, files, df_games_version, formatted_time):
    
    df_control = spark.createDataFrame([(
                                        extraction_time.strftime("%Y-%m-%d %H:%M:%S"),
                                        int(extraction_time.timestamp()),
                                        load_type,
                                        files,
                                        df_games_version,
                                        formatted_time)], schema=configuration["schema"])
    # Salva o novo registro da tabela de controle 
    (df_control
        .write
        .format('parquet')
        .mode('append')
        .save(configuration["control_table_path"])
    )



# Separa os dados a serem extraídos em chunks e o processa a extração de cada chunk
def extract_data_by_chunks(configuration, queue, search_type, extraction_time, extraction_date, spark, minio_client, data_buffer, df_games, files):

    print("Step 3: Processando a extração dos dados...")

    # Computa o número total de chunks
    total_chunks = df_games.agg(fn.max("chunk_num")).collect()[0][0] + 1

    # Processa a extração de cada chunk
    for chunk in range(total_chunks):
        print(f"Chunk {chunk + 1} de {total_chunks}.")
        
        print("Step 3.1: Obtendo o chunk de dados atual...")

        # Obtém o chunk de dados atual
        df_chunk = get_data_chunk(df_games, chunk)

        print("Step 3.2: Extraindo os dados do chunk atual...")

        # Extrai os dados do chunk atual
        extracted_data = asyncio.run(extract_data_chunk(search_type, spark, df_chunk, queue, configuration["max_concurrent_coroutines"], int(extraction_time.timestamp()), configuration["similarity_threshold"]))

        print("Step 3.3: Processando os resultados extraídos...")

        records = [json.loads(row) for row in extracted_data.toJSON().collect()]
        data_buffer.extend(records)

        print("Step 3.3: Processando os resultados extraídos... OK")

        if len(data_buffer) >= configuration["data_save_limit"]:
            
            print("Step 3.4: Salvando os dados extraídos...")

            # Incrementa o contador de arquivos
            files += 1

            # Salva o buffer em um arquivo JSON (limitado ao tamanho máximo definido)
            data_to_save = data_buffer[:configuration["data_save_limit"]]
            data_buffer = data_buffer[configuration["data_save_limit"]:]

            save_data_buffer(configuration["api_name"], extraction_date, minio_client, data_to_save, files)

            print("Step 3.5: Salvando os dados extraídos... OK")

        # RETIRAR: Para testes (single chunk)
        #if (chunk + 1) == 20:
        #    break

    print("Step 4: Processando a extração dos dados... OK")

    return data_buffer, files



# Realiza a carga inicial dos dados
def initial_load(configuration, extraction_time, extraction_date, spark, minio_client):
    load_type = 'Initial'

    # Cria buffer para acumular os resultados extraídos da API
    data_buffer = []

    # Inicializa o contador de arquivos
    files = 0

    # Cria fila para enfileirar os resultados
    queue = asyncio.Queue()

    # Obtém a lista de jogos a serem extraídos
    df_games, df_games_version = load_game_list(load_type, configuration["igdb_game_list_path"], spark, configuration["chunk_size"])

    # Separa os dados a serem extraídos em chunks e o processa a extração de cada chunk
    data_buffer, files = extract_data_by_chunks(configuration, queue, "GN", extraction_time, extraction_date, spark, minio_client, data_buffer, df_games, files)

    # Salva o buffer em um arquivo JSON (saldo remanescente)   
    if data_buffer:

        print("Step 4.1: Salvando os dados remanescentes...")

        # Incrementa o contador de arquivos
        files += 1

        # Salva o buffer em um arquivo JSON (saldo remanescente)
        save_data_buffer(configuration["api_name"], extraction_date, minio_client, data_buffer, files)

        print("Step 4.2: Salvando os dados remanescentes... OK")
        
    print(f"Foram importados {files} arquivos json para a Carga Inicial")

    return load_type, files, df_games_version



# Realiza a carga incremental dos dados
def incremental_load(configuration, extraction_time, extraction_date, spark, minio_client, df_control):
    load_type = 'Incremental'

    # Cria buffer para acumular os resultados extraídos da API
    data_buffer = []

    # Inicializa o contador de arquivos
    files = 0

    # Cria fila para enfileirar os resultados
    queue = asyncio.Queue()


    # Parte 1: Obtém os dados do How Long To Beat para os jogos já extraídos anteriormente

    # Obtém a lista de jogos a serem extraídos (com base na lista de jogos já extraídos do How Long To Beat)
    df_games_hltb, df_games_version = load_game_list(load_type, configuration["hltb_game_list_path"], spark, configuration["chunk_size"], api_name="HLTB")

    print("Número de jogos a serem extraídos: " + str(df_games_hltb.count()) + ".")

    # Separa os dados a serem extraídos em chunks e o processa a extração de cada chunk
    data_buffer, files = extract_data_by_chunks(configuration, queue, "ID", extraction_time, extraction_date, spark, minio_client, data_buffer, df_games_hltb, files)
 

    # Parte 2: Obtém os dados do How Long To Beat para os jogos atualizados do IGDB

    # Obtém a lista de jogos a serem extraídos (com base na lista de atualizações do IGDB)
    df_games_igdb, df_games_version = load_game_list(load_type, configuration["igdb_game_list_path"], spark, configuration["chunk_size"], api_name="IGDB", df_control=df_control)

    # Remove os registros que já se encontram na tabela HLTB
    df_games_igdb = df_games_igdb.join(df_games_hltb, df_games_igdb.id == df_games_hltb.id, how='left_anti').select("id", "name", "release_year", "chunk_num")
    
    print ("Número de jogos a serem extraídos (removendo os já consultados): " + str(df_games_igdb.count()) + ".")

    # Separa os dados a serem extraídos em chunks e o processa a extração de cada chunk
    data_buffer, files = extract_data_by_chunks(configuration, queue, "GN", extraction_time, extraction_date, spark, minio_client, data_buffer, df_games_igdb, files)
 

    # Salva o buffer em um arquivo JSON (saldo remanescente)
    if data_buffer:
            
        # Incrementa o contador de arquivos
        files += 1

        # Salva o buffer em um arquivo JSON (saldo remanescente)
        save_data_buffer(configuration["api_name"], extraction_date, minio_client, data_buffer, files)

    print(f"Foram importados {files} arquivos json para a Carga Incremental")

    return load_type, files, df_games_version



# Extrai os dados da API
def extract_data(configuration, extraction_time, extraction_date, spark, minio_client):

    # Obtém a tabela de controle de atualizações do endpoint
    df_control = get_control_table(configuration, spark)

    # Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental)
    if df_control.count() == 0:
        load_type, files, df_games_version = initial_load(configuration, extraction_time, extraction_date, spark, minio_client)
    else:
        load_type, files, df_games_version = incremental_load(configuration, extraction_time, extraction_date, spark, minio_client, df_control)
    
    return load_type, files, df_games_version



def main():
    # Obtém a sessão do Spark e as variáveis de configuração
    spark = create_spark_session()
    minio_client = get_minio_client()
    configuration = get_configuration()

    # Define o offset UTC para o Brasil (GMT-3)
    time_offset = timezone(timedelta(hours=-3))

    # Define a data e hora do início da extração para a tabela de controle de atualizações 
    extraction_time = datetime.now(time_offset)

    # Define a data de extração para particionamento no Lake
    extraction_date = extraction_time.strftime("%Y-%m-%d")
    
    # Métrica de tempo de execução (início da extração)
    start_time = time.time() 

    # Extrai os dados da API
    load_type, files, df_games_version = extract_data(configuration, extraction_time, extraction_date, spark, minio_client)

    # Métrica de tempo de execução (fim da extração)
    end_time = time.time()
    execution_time = end_time - start_time

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
    update_control_table(configuration, spark, extraction_time, load_type, files, df_games_version, formatted_time)

    print(f"Execução finalizada! Processo executado em {formatted_time}.")

if __name__ == "__main__":
    main()