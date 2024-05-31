# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as fn
from datetime import datetime, timedelta, timezone
from minio import Minio
import io, json, requests, time


# Define a sessão do Spark com os jars necessários para conexão com o MINIO
def create_spark_session():
    return (SparkSession.builder
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
        "api_name": 'igdb',
        "client_id": "nav10n96uf1vahn0sqiklmwuy331pz",
        "authorization": "3hlrk0kjcnm1im7hxmwrgmaz03z77u",
        "url": 'https://api.igdb.com/v4/',
        
        "rate_limit": 1 / 4,        # Requisições por segundo (limitação da API)
        "data_retrieve_limit": 500, # Número máximo de registros a serem recuperados por requisição
        "data_save_limit": 10000,   # Número máximo de registros a serem salvos por arquivo json
        
        "endpoints_inc": ["games", "genres", "game_modes", "player_perspectives",       # Endpoints para extração incremental
                           "platforms", "external_games", "keywords", "languages",
                           "language_supports", "language_support_types", "themes",
                           "franchises", "collections", "game_engines", "companies", 
                           "involved_companies", "game_localizations", "regions",
                           "release_dates", "release_date_statuses"],
        "endpoints_full": ["platform_families", "platform_logos", "multiplayer_modes",  # Endpoints para extração completa
                           "game_engine_logos", "company_logos", "websites", "age_ratings",
                           "age_rating_content_descriptions", "covers"],

        "bucket_path": 's3a://landing-zone/igdb/',  # Caminho do bucket no Minio
        "schema": StructType([
                StructField("Exec_date", StringType(), nullable=False),
                StructField("Exec_time", LongType(), nullable=False),
                StructField("Load_type", StringType(), nullable=False),
                StructField("Loaded_files", LongType(), nullable=False),
                StructField("Total_duration", StringType(), nullable=False)
        ])
    }
    return configuration




# Obtém a tabela de controle de atualizações de um endpoint específico (ou cria uma nova se ela não existir)
def get_control_table(configuration, spark, endpoint):
    control_table_path = configuration["bucket_path"] + endpoint + '/control_table/'
    
    try:
        return spark.read.parquet(control_table_path)
    except AnalysisException:
        return spark.createDataFrame([], schema=configuration["schema"])



# Realiza uma requisição POST para a API
def make_post_request(url, request):
    try:
        response = requests.post(url, **request)
        response.raise_for_status()     # Verifica se houve algum erro na requisição
        return response.json()          # Retorna a resposta em formato JSON
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição POST: {e}")
        return None



# Salva o buffer em um arquivo JSON no Lake
def save_data_buffer(api_name, extraction_date, minio_client, endpoint, data_buffer, files):
    
    # Nome do arquivo a ser salvo
    file_name = api_name + '/' + endpoint + '/' + extraction_date + '/' + api_name + '_' + endpoint + '_page_' + str(files).zfill(3) + '.json'

    # Transforma os dados em formato json
    json_data = json.dumps(data_buffer)

    # Salva o arquivo json no bucket
    minio_client.put_object('landing-zone', file_name, io.BytesIO(json_data.encode('utf-8')), len(json_data))



# Atualiza a tabela de controle de atualizações de um endpoint específico
def update_control_table(configuration, spark, endpoint, extraction_time, load_type, files, formatted_time):
        
        # Define o path da tabela de controle
        control_table_path = configuration["bucket_path"] + endpoint + '/control_table/'

        # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
        df_control = spark.createDataFrame([(
                                            extraction_time.strftime("%Y-%m-%d %H:%M:%S"),
                                            int(extraction_time.timestamp()),
                                            load_type,
                                            files,
                                            formatted_time)], schema=configuration["schema"])
        # Salva o novo registro da tabela de controle 
        (df_control
            .write
            .format('parquet')
            .mode('append')
            .save(control_table_path)
            )
        
        


# Extrai os dados de um endpoint específico, utilizando o método de extração incremental (para tabelas que possuem campo de atualização)
def extract_data_from_endpoint_inc(configuration, extraction_time, extraction_date, spark, minio_client, endpoint):
    
    # Buffer para acúmulo dos dados extraídos
    data_buffer = []

    # Métrica de tempo de execução (início da extração do endpoint)
    start_time_endpoint = time.time()

    # Obtém a tabela de controle de atualizações do endpoint
    df_control = get_control_table(configuration, spark, endpoint)

    # Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental) e a query a ser utilizada
    if df_control.count() == 0:
        load_type = 'Initial'
        
        # Query para solicitação dos dados de todos os dados (carga inicial)
        query = 'fields *; limit ' + str(configuration["data_retrieve_limit"]) + '; sort id asc;'

    else:
        load_type = 'Incremental'
        
        # Define a data e hora da última extração realizada
        last_execution = df_control.select(fn.max(fn.col("Exec_time")).alias("Latest_execution")).first()["Latest_execution"]
        
        # Query para solicitação dos dados atualizados (carga incremental)
        query = 'fields *; where updated_at > ' + str(last_execution) + '; limit ' + str(configuration["data_retrieve_limit"]) + '; sort id asc;'

    print("Iniciando endpoint: " + endpoint)

    files = 0
    offset = 0

    url_endpoint = configuration["url"] + endpoint

    while True:

        # Atualiza a query com o offset
        query_page = query + ' offset ' + str(offset) + ';'
        
        # Define os parâmetros da requisição
        request = {'headers': {'Client-ID': configuration["client_id"], 'Authorization': 'Bearer ' + configuration["authorization"]},'data': query_page}

        # Realiza a chamada via método POST
        data = make_post_request(url_endpoint, request)

        # Verifica se a resposta está vazia (erro ou fim dos dados)      
        if not data:
            break

        # Acumula os dados extraídos em um buffer
        data_buffer.extend(data)
    
        # Verifica se o buffer atingiu o limite de registros a serem salvos
        if len(data_buffer) >= configuration["data_save_limit"]:
            
            # Incrementa o contador de arquivos
            files += 1

            # Salva o buffer em um arquivo JSON
            save_data_buffer(configuration["api_name"], extraction_date, minio_client, endpoint, data_buffer, files)

            # Limpa o buffer
            data_buffer.clear()

        # Atualiza o offset para a próxima requisição e aguarda o rate limit
        offset += configuration["data_retrieve_limit"]
        time.sleep(configuration["rate_limit"])

    # Salva o saldo do buffer se houver dados
    if data_buffer:

        # Incrementa o contador de arquivos
        files += 1

        # Salva o buffer em um arquivo JSON
        save_data_buffer(configuration["api_name"], extraction_date, minio_client, endpoint, data_buffer, files)

    # Métrica de tempo de execução (fim da extração do endpoint)
    end_time_endpoint = time.time()
    execution_time_endpoint = end_time_endpoint - start_time_endpoint

    # Calcula o tempo de execução do endpoint
    hours, rem = divmod(execution_time_endpoint, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução do endpoint
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
    update_control_table(configuration, spark, endpoint, extraction_time, load_type, files, formatted_time)

    print(f"Foram importados {files} arquivos json para o endpoint '{endpoint}', em {formatted_time}.")



# Verifica se os dados no buffer são diferentes dos dados do último arquivo salvo (considerando estrutura e conteúdo)
def check_data_buffer(spark, endpoint, api_name, bucket_path, df_control, data_buffer):

    # Identifica a última data de gravação do endpoint (considere apenas Loades_files > 0)
    last_file = df_control.filter(fn.col("Loaded_files") > 0).select(fn.max(fn.col("Exec_time")).alias("Latest_execution")).first()["Latest_execution"]

    # Converte o timestamp para o formato YYYY-MM-DD, considerando o timezone do Brasil (GMT-3)
    extraction_date = datetime.fromtimestamp(last_file, timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
    
    # Carrega os dados do último arquivo salvo
    file_name = endpoint + '/' + extraction_date + '/' + api_name + '_' + endpoint + '_page_001.json'
    df_last_file = spark.read.json(bucket_path + file_name)

    # Cria um DataFrame com os dados do buffer para comparação do schema e conteúdo
    df_buffer = spark.createDataFrame(data_buffer)

    # Ordena as colunas dos DataFrames para garantir a comparação
    df_last_file = df_last_file.select(sorted(df_last_file.columns))
    df_buffer = df_buffer.select(sorted(df_buffer.columns))

    # Verifica se os dados no buffer são diferentes dos dados do último arquivo salvo (considerando estrutura e conteúdo)
    return df_last_file.schema != df_buffer.schema or df_last_file.subtract(df_buffer).count() > 0



# Extrai os dados de um endpoint específico, utilizando o método de extração completa (para tabelas que não possuem campo de atualização)
def extract_data_from_endpoint_full(configuration, extraction_time, extraction_date, spark, minio_client, endpoint):

    # Buffer para acúmulo dos dados extraídos
    data_buffer = []

    # Métrica de tempo de execução (início da extração do endpoint)
    start_time_endpoint = time.time()

    # Obtém a tabela de controle de atualizações do endpoint
    df_control = get_control_table(configuration, spark, endpoint)

    # Query para solicitação dos dados de todos os dados (carga completa)
    query = 'fields *; limit ' + str(configuration["data_retrieve_limit"]) + '; sort id asc;'

    print("Iniciando endpoint: " + endpoint)

    files = 0
    offset = 0

    url_endpoint = configuration["url"] + endpoint

    while True:

        # Atualiza a query com o offset
        query_page = query + ' offset ' + str(offset) + ';'
        
        # Define os parâmetros da requisição
        request = {'headers': {'Client-ID': configuration["client_id"], 'Authorization': 'Bearer ' + configuration["authorization"]},'data': query_page}

        # Realiza a chamada via método POST
        data = make_post_request(url_endpoint, request)

        # Verifica se a resposta está vazia (erro ou fim dos dados)      
        if not data:
            break

        # Acumula os dados extraídos em um buffer
        data_buffer.extend(data)
    
        # Atualiza o offset para a próxima requisição e aguarda o rate limit
        offset += configuration["data_retrieve_limit"]
        time.sleep(configuration["rate_limit"])

    # Salva o saldo do buffer se houver dados
    if data_buffer:

        # Se for a primeira extração ou se o buffer for diferente do último arquivo salvo, salva os dados em um arquivo JSON
        if df_control.count() == 0 or check_data_buffer(spark, endpoint, configuration["api_name"], configuration["bucket_path"], df_control, data_buffer):
            
            # Incrementa o contador de arquivos
            files += 1
    
            # Salva o buffer em um arquivo JSON
            save_data_buffer(configuration["api_name"], extraction_date, minio_client, endpoint, data_buffer, files)

    # Métrica de tempo de execução (fim da extração do endpoint)
    end_time_endpoint = time.time()
    execution_time_endpoint = end_time_endpoint - start_time_endpoint

    # Calcula o tempo de execução do endpoint
    hours, rem = divmod(execution_time_endpoint, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução do endpoint
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
    update_control_table(configuration, spark, endpoint, extraction_time, 'Full', files, formatted_time)

    print(f"Foram importados {files} arquivos json para o endpoint '{endpoint}', em {formatted_time}.")



# Extrai os dados da API
def extract_data(configuration, extraction_time, extraction_date, spark, minio_client):
    
    # Extrai os dados dos endpoints para extração incremental
    for endpoint in configuration["endpoints_inc"]:
        extract_data_from_endpoint_inc(configuration, extraction_time, extraction_date, spark, minio_client, endpoint)

    # Extrai os dados dos endpoints para extração completa
    for endpoint in configuration["endpoints_full"]:
        extract_data_from_endpoint_full(configuration, extraction_time, extraction_date, spark, minio_client, endpoint)



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
    extract_data(configuration, extraction_time, extraction_date, spark, minio_client)

    # Métrica de tempo de execução (fim da extração)
    end_time = time.time()
    execution_time = end_time - start_time

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    print(f"Carga finalizada! Processo executado em {formatted_time}.")

if __name__ == "__main__":
    main()