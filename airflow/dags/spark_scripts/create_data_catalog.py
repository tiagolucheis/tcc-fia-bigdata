# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from minio import Minio
from trino import dbapi
import time



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



# Define a conexão com o Trino
def get_trino_connection():
    conn = dbapi.connect(host="trino", port=8080, user="trino")
    conn.autocommit = True
    return conn



# Define o Mapeamento de tipos entre o Spark e o Trino
def map_types():
    type_mapping = {
        'StringType': 'varchar',
        'IntegerType': 'integer',
        'BooleanType': 'boolean',
        'LongType': 'bigint',
        'DoubleType': 'double',
        'TimestampType': 'timestamp',
        'ArrayType(StringType,true)': 'array(varchar)',
        'ArrayType(IntegerType,true)': 'array(integer)',
        'ArrayType(LongType,true)': 'array(bigint)'
    }
    return type_mapping



# Define as variáveis parametrizáveis do script
def get_configuration():
    configuration = {
        "control_buckets":     ['landing-zone', 'raw', 'context', 'trust'],     # Lista de buckets com tabelas de controle de atualização
        "data_buckets":        ['raw', 'context', 'trust']                      # Lista de buckets com tabelas de dados
    }
    return configuration



# Obtém a lista de tabelas existentes no bucket
def get_source_tables(minio_client, filter_rule, bucket):
    objects = minio_client.list_objects(bucket, recursive=True)
    tables = {}
    
    # Ajusta o nome da Landinz Zone, em função da sintaxe do Trino
    if bucket == 'landing-zone':
        bucket = 'landing_zone'

    for obj in objects:
    
        # Analisa apenas os caminhos que contém arquivos parquet dentro de um subdiretório chamado 'delta'
        if obj.object_name.endswith('snappy.parquet') and filter_rule in obj.object_name:

            parts = obj.object_name.split('/')
            
            table_name = bucket + '_' + '_'.join(parts[0:len(parts)-2])
            table_path = '/'.join(parts[0:len(parts)-1]) + '/'

            # Adiciona a tabela à lista de tabelas, se ainda não existir
            if table_name not in tables:
                tables[table_name] = table_path
            
    return tables



# Cria a tabela de controle de atualização no Trino
def create_table(spark, cursor, bucket, table, path, type_mapping):

    print(f"Criando/atualizando a tabela de controle de atualização para a tabela {table} do bucket {bucket}...")

    # Obtém o schema da tabela parquet
    df = spark.read.parquet(path)
    schema = df.schema

    # Aplica o mapeamento de tipos entre o Spark e o Trino
    for field in schema:
        field.dataType = type_mapping[str(field.dataType)]

    # Define as colunas da tabela para o comando SQL
    columns = ', '.join([f'{field.name} {field.dataType}' for field in schema])

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS control.default.{table} (
            {columns}
        )
        WITH (
            format = 'parquet',
            external_location = '{path}'
        ) 
    ''')
    
    print(cursor.fetchall())



# Cria uma tabela no Trino, a partir de um arquivo Delta Parquet
def register_table(cursor, bucket, table, path):

    # Registrar a tabela no Trino
    cursor.execute(f'''
        CALL minio.system.register_table(
            '{bucket}',
            '{table}',
            '{path}'
        )          
    ''')

    print(cursor.fetchall())



# Cria o schema no Trino para cada bucket
def create_schema(trino_connection, buckets):

    # Obtém o cursor do Trino para execução dos comandos SQL
    cursor = trino_connection.cursor()

    # Cria o schema para cada bucket
    for bucket in buckets:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS minio.{bucket}")

    cursor.close()



# Cria as tabelas no Trino para cada bucket
def create_tables(spark, minio_client, trino_connection, buckets, filter_rule, type_mapping=""):
    
    # Obtém o cursor do Trino para execução dos comandos SQL
    cursor = trino_connection.cursor()

    for bucket in buckets:

        print(f"Criando/atualizando as tabelas do schema {bucket}...")
        
        # Define o Catálogo e o Schema do Trino
        if filter_rule == 'delta':
            catalog_schema = f"minio.{bucket}"
        elif filter_rule == 'control_table':
            catalog_schema = "control.default"
        else:
            print(f"Erro ao obter a lista de tabelas do bucket {bucket}.")
            break

        # Obtém a lista de tabelas do bucket
        cursor.execute(f'SHOW TABLES IN {catalog_schema}')
        existing_tables = [row[0] for row in cursor.fetchall()]

        # Obtém a lista de tabelas do bucket
        tables = get_source_tables(minio_client, filter_rule, bucket)

        # Cria as tabelas no Trino
        for table, path in tables.items():

            print(f"Criando/atualizando a tabela {table}, cujo caminho é {path}...")

            # Define o Path do arquivo Delta Parquet
            delta_path = f"s3a://{bucket}/{path}"

            try:

                # Ajusta o nome da tabela, caso seja uma tabela de controle de atualização
                if filter_rule == 'control_table':
                    table = 'ctrl_' + table

                # Verifica se a tabela já existe no Trino
                if table in existing_tables:

                    if filter_rule == 'delta':
                    
                        # Tabela já existe no Trino, então procede com a comparação e possível atualização
                        cursor.execute(f'SHOW COLUMNS FROM {catalog_schema}.{table}')
                        existing_columns = [row[0] for row in cursor.fetchall()]

                        # Lê o arquivo Delta Parquet
                        delta_table = DeltaTable.forPath(spark, delta_path).toDF()
                        
                        # Lê os metadados do arquivo Delta Parquet
                        schema = delta_table.schema

                        # Se a tabela for a game_age_ratings do schema trust, imprime as colunas
                        if table == 'trust_game_age_ratings':
                            print(f"Colunas da tabela delta {table}: {schema.names}")
                            print(f"Colunas da tabela {table} no catálogo minio: {existing_columns}")

                        # Compara as colunas da tabela com as colunas existentes no Trino
                        if existing_columns != schema.names:
                            # Se as colunas forem diferentes, recria a tabela no Trino
                            cursor.execute(f"DROP TABLE IF EXISTS {catalog_schema}.{table}")
                            create_table(cursor, bucket, table, delta_path)

                            print(f"Tabela {table} atualizada com sucesso no catálogo minio, schema {bucket}.")

                        else:
                            print(f"Tabela {table} já existe no catálogo minio, schema {bucket}.")

                    else: 
                        print(f"Tabela {table} já existe no catálogo control.")

                else:
                    
                    if filter_rule == 'delta':

                        # Tabela não existe no Trino, então cria a tabela
                        register_table(cursor, bucket, table, delta_path)

                        print(f"Tabela {table} criada com sucesso no catálogo minio, schema {bucket}.")

                    else:

                        # Cria a tabela de controle de atualização
                        create_table(spark, cursor, bucket, table, delta_path, type_mapping)
                        
                        print(f"Tabela {table} criada com sucesso no catálogo control.")


            except Exception as e:
                print(f"Erro ao criar/atualizar a tabela {table} no schema {bucket}.")
                print(e)
            
        cursor.close()



def main():
    # Inicializa as conexões com o Minio e o Trino, e obtém as variáveis de configuração 
    spark = create_spark_session()
    minio_client = get_minio_client()
    trino_connection = get_trino_connection()
    configuration = get_configuration()
    type_mapping = map_types()
    
    # Métrica de tempo de execução (início da criação do catálogo)
    start_time = time.time() 

    # Cria o schema para cada bucket
    create_schema(trino_connection, configuration['data_buckets'])

    # Cria as tabelas de cada bucket no Trino
    create_tables(spark, minio_client, trino_connection, configuration['data_buckets'], 'delta')

    # Cria as tabelas de controle de atualização no Trino
    create_tables(spark, minio_client, trino_connection, configuration['control_buckets'], 'control_table', type_mapping)

    # Métrica de tempo de execução (fim da criação do catálogo)
    end_time = time.time()
    execution_time = end_time - start_time

    # Calcula o tempo de execução
    hours, rem = divmod(execution_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Formata o tempo de execução
    formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    print(f"Criação/Atualização do Catálogo de Dados finalizada! Processo executado em {formatted_time}.")

if __name__ == "__main__":
    main()