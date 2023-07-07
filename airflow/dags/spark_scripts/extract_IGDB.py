# Define os imports necessários para a execução do código
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
import os, time, json
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



# Lista de Endpoints a serem carregados
endpoints = ["games"]

# Define a data de extração para particionamento no Lake
extraction_date = time.strftime("%Y-%m-%d")

# Dados de acesso e URL da API
client_id = "rtpo9gi65ed6me23fslamjdmqme962"
authorization = "ojr9ucic9n7670dz3sbdba5kb2wa06"
api_prefix = "IGDB_"
url = 'https://api.igdb.com/v4/'

# Limite de registros a serem lidos por request da API
data_retrieve_limit = 500

# Query para solicitação dos dados
query = 'fields *; limit ' + str(data_retrieve_limit) + '; sort id asc;'

# Limite de requests a serem feitos por segundo (por limitação da API)
rate_limit = 0 # 1 / 4 (desabilitado em função do delay do S3)



# Executa um loop para varrer todos os endpoints e obter seus dados

inicio_exec = time.time()

for endpoint in endpoints:

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
            print("Erro na requisição: " + response.status_code)
            break

        # Verifica se a resposta está vazia (fim dos dados)      
        if not data:
            break

        # Incrementa o contador de páginas
        page += 1
        
        # Cria o dataframe com os dados extraídos
        df = spark.createDataFrame(data)

        # Salva o dataframe em um arquivo json no diretório especificado
        df.write.json('s3a://raw/igdb/' + endpoint + '/' + extraction_date + '/' + api_prefix + endpoint + '_page_' + str(page).zfill(3) + '.json', mode='overwrite')

        # Atualiza o offset para a próxima requisição e aguarda o rate limit      
        offset += data_retrieve_limit
        time.sleep(rate_limit)

    print(f"Foram importados {page} arquivos json para o endpoint '{endpoint}'")

fim_exec = time.time()
tempo_exec = fim_exec - inicio_exec

# Calcula o tempo de execução
horas = int(tempo_exec // 3600)
minutos = int((tempo_exec % 3600) // 60)
segundos = int(tempo_exec % 60)

print(f"Carga Inicial finalizada! Processo executado em {horas:02d}:{minutos:02d}:{segundos:02d}.")