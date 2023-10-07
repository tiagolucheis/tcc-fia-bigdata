# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window 
from delta.tables import DeltaTable
import pyspark.sql.functions as fn
from datetime import datetime, timedelta, timezone
from minio import Minio
import time, argparse

# Define os argumentos do script
parser = argparse.ArgumentParser(description="Script para Leitura dos dados em JSON e conversão para Deltaframe")

parser.add_argument("--api_name", type=str, required=True, help="Nome da API")
parser.add_argument("--endpoints", type=str, required=True, help="Lista de Endpoints")
parser.add_argument("--mod_date_col", type=str, required=True, help="Coluna de controle de atualização do dado")


args = parser.parse_args()


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

print(args.endpoints)

# Define o cliente do Minio
def get_minio_client():
    return Minio("minio:9000", access_key="aulafia", secret_key="aulafia@123", secure=False)



# Define as variáveis parametrizáveis do script
def get_configuration():

    source_bucket_path = 's3a://landing-zone/' + args.api_name + '/'
    target_bucket_path = 's3a://raw/' + args.api_name + '/'

    endpoints = args.endpoints.split()

    configuration = {
        "api_name": args.api_name,
        
        "endpoints": endpoints,
        
        "source_bucket_name": 'landing-zone',           # Nome do bucket de origem
        "source_bucket_path": source_bucket_path,       # Caminho do bucket de origem
        "target_bucket_path": target_bucket_path,       # Caminho do bucket de destino

        "schema": StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Loaded_date", StringType(), nullable=False),
            StructField("Read_Rows", StringType(), nullable=False),
            StructField("Table_Rows", StringType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])
    }
    return configuration



# Obtém a tabela de controle de atualizações da origem (landing-zone) de um endpoint específico 
def get_source_control_table(source_bucket_path, spark, endpoint):
    return spark.read.parquet(source_bucket_path + endpoint + '/control_table/')



# Obtém a tabela de controle de atualizações do destino (raw) de um endpoint específico (ou cria uma nova se ela não existir)
def get_target_control_table(target_bucket_path, spark, endpoint, schema):
    try:
        return spark.read.parquet(target_bucket_path + endpoint + '/control_table/')
        
    except AnalysisException:
        return spark.createDataFrame([], schema=schema)



# Carrega o Dataframe com os dados atuais e define a data e hora da última leitura realizada
def get_actual_data(spark, delta_table_path, df_target_control):
    try:
        df_actual = DeltaTable.forPath(spark, delta_table_path)
        last_read = df_target_control.select(fn.max(fn.col("Loaded_date")).alias("Latest_read")).collect()[0][0]
        
    except AnalysisException:
        df_actual = None
        last_read = None
    
    return df_actual, last_read



# Obtém as datas de execução que serão lidas
def get_exec_dates(df_source_control, df_actual, last_read):
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

    return exec_dates



# Lista os arquivos JSON no MinIO
def list_objects(minio_client, source_bucket_name, api_name, endpoint, date):
    json_files = []
    objects = minio_client.list_objects(source_bucket_name, prefix=api_name + '/' + endpoint + '/' + date + "/")
    
    for obj in objects:
        path = f"s3a://{source_bucket_name}/{obj.object_name}"
        json_files.append(path)
    
    return json_files



# Lê e combina os arquivos JSON em um DataFrame
def read_json(spark, json_files):
    df_read = None
        
    # Loop para ler cada arquivo JSON e combinar os DataFrames
    for json_file in json_files:

        print("Lendo arquivo: " + json_file)

        df_temp = spark.read.json(json_file, encoding="UTF-8")
        
        # Se o DataFrame inicial estiver vazio, atribui o DataFrame atual
        if df_read is None:
            df_read = df_temp
        
        # Caso contrário, combina o DataFrame atual com o DataFrame anterior
        else:
            df_read = df_read.unionByName(df_temp, allowMissingColumns=True)
    
    print(df_read)

    # Remove os registros duplicados, se houver (caso ocorram múltiplas alterações no mesmo registro durante o período de extração)
    if df_read.count() != df_read.select("id").distinct().count():    
        
        window_spec = Window.partitionBy("id").orderBy(fn.col(args.mod_date_col).desc())
        df_read = (
            df_read
            .withColumn("row_number", fn.row_number().over(window_spec))
            .filter(fn.col("row_number") == 1)
            .drop("row_number")
        )
        
    return df_read



# Salva o DataFrame como uma tabela Delta
def save_delta_table(df, delta_table_path, format="delta", mode="overwrite"):
    df.write.format(format).mode(mode).save(delta_table_path)


# Realiza o merge entre o DataFrame atual e a tabela Delta
def merge_dataframes(spark, df_actual, df_date, delta_table_path):
    # Se o DataFrame inicial estiver vazio, apenas atribui o DataFrame atual
    if (df_actual is None):
        df_actual = df_date
        
        # Salva os dados como uma tabela Delta (overwrite sobrescreverá a tabela existente)
        save_delta_table(df_actual, delta_table_path)
        return DeltaTable.forPath(spark, delta_table_path)
        
    # Caso contrário, combina o DataFrame atual com o DataFrame anterior
    else:
        print("Número de registros na tabela Delta (antes do merge): " + str(df_actual.toDF().count()) + ".")
        
        # Verifica se o Schema do DataFrame atual é igual ao Schema da tabela Delta
        if (df_actual.toDF().schema != df_date.schema):
            schema_actual = set(df_actual.toDF().schema.fieldNames())
            schema_date = set(df_date.schema.fieldNames())
            missing_columns = schema_actual - schema_date  
                
            # Adiciona as colunas faltantes ao DataFrame atual
            for column in missing_columns:
                df_date = df_date.withColumn(column, fn.lit(None))
                    
        # Realiza o merge entre o DataFrame atual e a tabela Delta            
        df_actual.alias("actualData") \
            .merge(source = df_date.alias("updatedData"),
                   condition = fn.expr("actualData.id = updatedData.id")) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        print("Número de registros na tabela Delta (após o merge): " + str(df_actual.toDF().count()) + ".")
        # Salva o resultado do merge na tabela Delta (overwrite sobrescreverá a tabela existente)
        save_delta_table(df_actual.toDF(), delta_table_path)
        
        return df_actual



# Atualiza a tabela de controle com a data e hora da leitura
def update_target_control_table(spark, configuration, df_target_control, endpoint, read_time, date, read_rows, table_rows, formatted_time):
    # Cria um DataFrame com o novo registro da tabela de controle
    df_control = spark.createDataFrame([(
                                            read_time.strftime("%Y-%m-%d %H:%M:%S"),
                                            int(read_time.timestamp()),
                                            date,
                                            read_rows,
                                            table_rows,
                                            formatted_time)], schema=df_target_control.schema)
    
    # Salva o novo registro da tabela de controle 
    (df_control
        .write
        .format('parquet')
        .mode('append')
        .save(configuration["target_bucket_path"] + endpoint + '/control_table/')
        )


# Lê os dados de um endpoint específico
def read_data_endpoint(spark, minio_client, configuration, read_time, endpoint):
    print("Iniciando tabela: " + endpoint) 
    
    # Métrica de tempo de leitura (início da leitura do endpoint)
    start_time_endpoint = time.time()

    # Obtém as tabelas de controle de atualizações do endpoint (landong-zone e raw)
    df_source_control = get_source_control_table(configuration["source_bucket_path"], spark, endpoint)
    df_target_control = get_target_control_table(configuration["target_bucket_path"], spark, endpoint, configuration["schema"])

    # Define o caminho do bucket de destino
    delta_table_path = configuration["target_bucket_path"] + endpoint + '/delta/'

    # Total de registros lidos
    total_rows = 0

    # Carrega o Dataframe com os dados atuais e define a data e hora da última leitura realizada
    df_actual, last_read = get_actual_data(spark, delta_table_path, df_target_control)

    print("Última data lida: " + str(last_read) + ".")

    # Obtém as datas de execução que serão lidas
    exec_dates = get_exec_dates(df_source_control, df_actual, last_read)

    print("Datas de execução que serão lidas: " + str(exec_dates) + ".")

    for date in exec_dates:
        print("Iniciando leitura de : " + date)
    
        # Métrica de tempo de leitura (início da leitura da data)
        start_time_date = time.time()

        # Lista os arquivos JSON no MinIO
        json_files = list_objects(minio_client, configuration["source_bucket_name"], configuration["api_name"], endpoint, date)

        # Lê e combina os arquivos JSON em um DataFrame
        df_date = read_json(spark, json_files)
        
        print ("Leitura finalizada. Iniciando merge com a tabela Delta. Contagem de registros: " + str(df_date.count()) + ".")
        
        # Realiza o merge entre o DataFrame atual e a tabela Delta
        df_actual = merge_dataframes(spark, df_actual, df_date, delta_table_path)

        # Métrica de tempo de leitura (fim da leitura da data)
        end_time_date = time.time()
        execution_time_date = end_time_date - start_time_date

        # Calcula o tempo de execução
        hours, rem = divmod(execution_time_date, 3600)
        minutes, seconds = divmod(rem, 60)

        # Formata o tempo de execução
        formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        # Atualiza o total de registros lidos
        total_rows += df_date.count()

        # Atualiza a tabela de controle com a data e hora da leitura
        update_target_control_table(spark, configuration, df_target_control, endpoint, read_time, date, df_date.count(), df_actual.toDF().count(), formatted_time)

        print("Leitura da data " + date + " finalizada! Processo executado em " + formatted_time + ".")

    # Métrica de tempo de leitura (fim da leitura do endpoint)
    end_time_endpoint = time.time()
    execution_time_endpoint = end_time_endpoint - start_time_endpoint

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time_endpoint, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    print("Leitura da tabela " + endpoint + " finalizada! Processo executado em " + formatted_time + ".")
    print("Total de registros lidos: " + str(total_rows) + ".")



# Lê os dados salvos no MINIO em formato JSON e salva em formato Delta
def read_data(spark, minio_client, configuration, read_time):
    for endpoint in configuration["endpoints"]:
        read_data_endpoint(spark, minio_client, configuration, read_time, endpoint)



def main():
    # Obtém a sessão do Spark e as variáveis de configuração
    spark = create_spark_session()
    minio_client = get_minio_client()
    configuration = get_configuration()
    
    # Define o offset UTC para o Brasil (GMT-3)
    time_offset = timezone(timedelta(hours=-3))

    # Define a data e hora do início da leitura para a tabela de controle de atualizações 
    read_time = datetime.now(time_offset)

    # Métrica de tempo de execução (início da leitura)
    start_time = time.time()

    # TO-DO: Implementar o loop para cada endpoint
    read_data(spark, minio_client, configuration, read_time)

    # Métrica de tempo de execução (fim da leitura)
    end_time = time.time()
    execution_time = end_time - start_time

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    print(f"Leitura finalizada! Processo executado em {formatted_time}.")   

if __name__ == "__main__":
    main()