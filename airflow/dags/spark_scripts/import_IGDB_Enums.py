# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as fn
from datetime import datetime, timedelta, timezone
from minio import Minio
import time, os, re



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



# Define as variáveis parametrizáveis do script
def get_configuration():
    configuration = {
        "api_name": "igdb_enums",
        
        "file_name_pattern": r'igdb_enum_([\w_]+)_([\w]+)\.csv',    # Padrão de nomenclatura para os arquivos
                            
        "source_bucket_name": 'landing-zone',                       # Nome do bucket de origem
        "source_bucket_path": 's3a://landing-zone/igdb_enums/',     # Caminho do bucket de origem
        "target_bucket_path": 's3a://raw/igdb_enums/',              # Caminho do bucket de destino

        "schema": StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Table_Rows", StringType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])
    }
    return configuration



# Define o cliente do Minio
def get_minio_client():
    return Minio("minio:9000", access_key="aulafia", secret_key="aulafia@123", secure=False)



# Retorna a lista de objetos no bucket do Minio
def list_objects(file_name_pattern, minio_client, bucket_name, prefix):

    # Obtém a lista de objetos no bucket
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    # Filtra os objetos com base no padrão de nomenclatura
    return [os.path.basename(obj.object_name) for obj in objects if re.match(file_name_pattern, os.path.basename(obj.object_name))]



# Obtém a tabela de controle de atualizações (ou cria uma nova se ela não existir)
def get_control_table(control_table_path, spark, schema):
    try:
        return spark.read.parquet(control_table_path)
    except AnalysisException:
        return spark.createDataFrame([], schema=schema)



# Verifica se o arquivo foi modificado
def check_file_modification(file_name, minio_client, df_control, bucket_name):
    
    # Obtém a data de modificação do arquivo no Minio
    modified_time = int((minio_client.stat_object(bucket_name, file_name).last_modified).timestamp())
    
    # Obtém a última data de leitura registrada na tabela de controle
    last_read_time = df_control.orderBy(fn.desc("Exec_time")).select("Exec_time").first()

    if last_read_time:
        # Se o arquivo foi modificado depois da última leitura, retorne True
        return modified_time > last_read_time["Exec_time"]
    
    # Se o arquivo nunca foi lido, retorne True
    else:
        return True

    


# Atualiza a tabela delta raw do Enum
def update_delta_table(spark, file_name, file_path, table_path):
    df = spark.read.csv(file_path + file_name, header=True, sep=';')
    
    df.write.format("delta").mode("overwrite").save(table_path)

    return df.count()



# Atualiza a tabela de controle com a data e hora da importação
def update_control_table(spark, schema, target_control_path, import_time, table_rows, formatted_time):
    # Cria um DataFrame com o novo registro da tabela de controle
    df_control = spark.createDataFrame([(
                                            import_time.strftime("%Y-%m-%d %H:%M:%S"),
                                            int(import_time.timestamp()),
                                            table_rows,
                                            formatted_time)], schema=schema)
    
    # Salva o novo registro da tabela de controle 
    (df_control
        .write
        .format('parquet')
        .mode('append')
        .save(target_control_path)
        )



# Verifica se há arquivos para importação e executa o processo de importação
def check_and_import_files(spark, minio_client, configuration, files_to_check, import_time):

    imported_files = 0

    for file_name in files_to_check:

        print("Iniciando verificação de: ", file_name)

        # Métrica de tempo de importação (início da verificação e importação do arquivo)
        start_time_file = time.time()

        # Obtém o nome da tabela de referência e o nome da tabela de enum, a partir do nome do arquivo
        match = re.match(configuration["file_name_pattern"], file_name)
        if match:
            table_ref = match.group(1)
            table_enum = match.group(2)

            # Define o caminho da tabela de controle de atualizações
            table_path = configuration["target_bucket_path"] + table_ref + '/' + table_enum
    
            # Obtém a tabela de controle de atualizações (ou cria uma nova se ela não existir)
            df_control = get_control_table(table_path + '/control_table/', spark, configuration["schema"])

            # Verifica se o arquivo foi modificado
            if check_file_modification(configuration["api_name"] + '/' + file_name, minio_client, df_control, configuration["source_bucket_name"]):
                
                # Atualiza a tabela delta raw correspondente
                table_rows = update_delta_table(spark, file_name, configuration["source_bucket_path"], table_path + '/delta/')
                imported_files += 1

                # Métrica de tempo de leitura (fim da leitura da data)
                end_time_file = time.time()
                execution_time_file = end_time_file - start_time_file

                # Calcula o tempo de execução
                hours, rem = divmod(execution_time_file, 3600)
                minutes, seconds = divmod(rem, 60)

                # Formata o tempo de execução
                formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

                # Atualiza a tabela de controle de atualizações
                update_control_table(spark, configuration["schema"], table_path + '/control_table/', import_time, table_rows, formatted_time)
                
                print(f"O arquivo {file_name} foi modificado e a tabela delta raw correspondente foi atualizada.")
            else:
                print(f"O arquivo {file_name} não foi modificado.")
        
        else:
            print(f"O arquivo {file_name} não está no padrão de nomenclatura esperado.")
    
    return imported_files



def main():
    # Obtém a sessão do Spark e as variáveis de configuração
    spark = create_spark_session()
    minio_client = get_minio_client()
    configuration = get_configuration()

    # Define o offset UTC para o Brasil (GMT-3)
    time_offset = timezone(timedelta(hours=-3))

    # Define a data e hora do início da importação para a tabela de controle de atualizações 
    import_time = datetime.now(time_offset)

    # Métrica de tempo de execução (início da importação)
    start_time = time.time()

    # Obtém a lista de objetos no bucket do Minio
    files_to_check = list_objects(configuration["file_name_pattern"], minio_client, configuration["source_bucket_name"], configuration["api_name"])

    print("Arquivos para verificar: ", files_to_check)
    
    # Verifica se há arquivos para verificação
    if len(files_to_check) > 0:

        # Verifica se há arquivos para importação e executa o processo de importação
        imported_files = check_and_import_files(spark, minio_client, configuration, files_to_check, import_time)
        print("Arquivos modificados e importados: ", imported_files)
  
    else:
        print("Não há arquivos para importação")

    # Métrica de tempo de execução (fim da importação)
    end_time = time.time()
    execution_time = end_time - start_time

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    print(f"Processo finalizado em {formatted_time}.")   

if __name__ == "__main__":
    main()