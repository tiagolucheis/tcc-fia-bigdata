# Define os imports necessários para a execução do código
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import time
from datetime import datetime, timedelta
from requests import post

# Define a sessão do Spark com os jars necessários para conexão com o MINIO
spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )



# Define o nome da API de onde serão extraídos os dados
api_name = 'igdb'

# Dados de acesso e URL da API
client_id = "nav10n96uf1vahn0sqiklmwuy331pz"
authorization = "hvphbzal24563rj5km9s4381jsphis"
url = 'https://api.igdb.com/v4/'

# Limite de registros a serem lidos por request da API
data_retrieve_limit = 500

# Limite de requests a serem feitos por segundo (por limitação da API)
rate_limit = 1 / 4 # (desabilitado em função do delay do S3)

# Limite de registros a serem salvos por arquivo json
data_save_limit = 10000
data_buffer = []

# Lista de Endpoints a serem carregados
endpoints = ["games", "genres", "game_modes", "player_perspectives", "platforms", "external_games"]

# TO-DO: Definir método para obter dados de Endpoint que não possui o campo "updated_at" (ex: "multiplayer_modes")

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

# Define a data e hora do início da extração para a tabela de controle de atualizações 
extraction_time = datetime.now() - timedelta(hours=3) #GMT -0300 (Horário Padrão de Brasília)

# Define a data de extração para particionamento no Lake
extraction_date = extraction_time.strftime("%Y-%m-%d")



# Executa um loop para varrer todos os endpoints e obter seus dados

start_time = time.time()

for endpoint in endpoints:
    
    start_time_endpoint = time.time()

    control_table_path = bucket_path + endpoint + '/control_table/'
    
    # Lê a tabela de controle ou cria uma nova se ela não existir
    try:
        df_control = spark.read.parquet(control_table_path)
        
    except AnalysisException:
        df_control = spark.createDataFrame([], schema=schema)
    
    # Verifica se a tabela de controle está vazia, definindo o tipo de carga (inicial ou incremental) e a query a ser utilizada 
    if df_control.count() == 0:
        load_type = 'Initial'
        
        # Query para solicitação dos dados de todos os dados (carga inicial)
        query = 'fields *; limit ' + str(data_retrieve_limit) + '; sort id asc;'
        
    else:
        load_type = 'Incremental'
        
        # Define a data e hora da última extração realizada
        last_execution = df_control.select(fn.max(fn.col("Exec_time")).alias("Latest_execution")).first()["Latest_execution"]
        
        # Query para solicitação dos dados atualizados (carga incremental)
        query = 'fields *; where updated_at > ' + str(last_execution) + '; limit ' + str(data_retrieve_limit) + '; sort id asc;'

    print("Iniciando endpoint: " + endpoint) 
    
    page = 0
    offset = 0
    
    url_endpoint = url + endpoint
    
    while True:

        # Atualiza a query com o offset
        query_page = query + ' offset ' + str(offset) + ';'

        # Define os parâmetros da requisição        
        request = {'headers': {'Client-ID': client_id, 'Authorization': 'Bearer ' + authorization},'data': query_page}

        # Realiza a chamada via método POST
        response = post(url_endpoint, **request)

        # Verificar o código de status da resposta
        if response.status_code == 200:
            data = response.json()
        else:
            print("Erro na requisição: " + str(response.status_code))
            break

        # Verifica se a resposta está vazia (fim dos dados)      
        if not data:
            break
        
        # Cria o dataframe com os dados extraídos
        df = spark.createDataFrame(data)

        # Adiciona os dados ao buffer
        data_buffer.extend(data)

        # Verifica se o buffer atingiu o limite de registros a serem salvos
        if len(data_buffer) >= data_save_limit:

            # Incrementa o contador de páginas
            page += 1

            # Salva o dataframe em um arquivo JSON no diretório especificado
            df_buffer = spark.createDataFrame(data_buffer)
            df_buffer.write.json(bucket_path + endpoint + '/' + extraction_date + '/' + api_name + '_' + endpoint + '_page_' + str(page).zfill(3) + '.json', mode='overwrite')
            data_buffer = []

        # Atualiza o offset para a próxima requisição e aguarda o rate limit      
        offset += data_retrieve_limit
        time.sleep(rate_limit)

    # Salva o saldo do buffer se houver dados
    if data_buffer:

        # Incrementa o contador de páginas
        page += 1

        # Salva o dataframe em um arquivo JSON no diretório especificado
        df_buffer = spark.createDataFrame(data_buffer)
        df_buffer.write.json(bucket_path + endpoint + '/' + extraction_date + '/' + api_name + '_' + endpoint + '_page_' + str(page).zfill(3) + '.json', mode='overwrite')

    end_time_endpoint = time.time()
    execution_time_endpoint = end_time_endpoint - start_time_endpoint

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time_endpoint, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    # Atualiza a tabela de controle com a data e hora da extração e o tipo de carga
    df_control = spark.createDataFrame([(
                                        extraction_time.strftime("%Y-%m-%d %H:%M:%S"),
                                        int(extraction_time.timestamp()),
                                        load_type,
                                        page,
                                        formatted_time)], schema=schema)
    # Salva o novo registro da tabela de controle 
    (df_control
        .write
        .format('parquet')
        .mode('append')
        .save(control_table_path)
        )
    
    print(f"Foram importados {page} arquivos json para o endpoint '{endpoint}', em {formatted_time}.")

end_time = time.time()
execution_time = end_time - start_time

# Calcula o tempo de execução
hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)

# Formata o tempo de execução
formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

print(f"Carga finalizada! Processo executado em {formatted_time}.")