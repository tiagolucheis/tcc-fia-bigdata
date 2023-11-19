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
    conn = dbapi.connect(host="trino", port=8080, user="trino", catalog="minio")
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
        "buckets": ['raw', 'context', 'trust']              # Lista de buckets a serem criados
    }
    return configuration



# Obtém a lista de tabelas existentes no bucket
def get_source_tables(minio_client, bucket):
    objects = minio_client.list_objects(bucket, recursive=True)
    tables = {}
    
    for obj in objects:
    
        # Analisa apenas os caminhos que contém arquivos parquet dentro de um subdiretório chamado 'delta'
        if obj.object_name.endswith('snappy.parquet') and 'delta' in obj.object_name:
            
            parts = obj.object_name.split('/')

            table_name = '_'.join(parts[0:len(parts)-2])
            table_path = '/'.join(parts[0:len(parts)-1]) + '/'

            # Adiciona a tabela à lista de tabelas, se ainda não existir
            if table_name not in tables:
                tables[table_name] = table_path
            
    return tables



# Cria uma tabela no Trino, a partir de um arquivo Delta Parquet
def create_table(cursor, bucket, table, path, columns):

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

    print("Buckets a serem criados/atualizados:")
    print(buckets)

    # Cria o schema para cada bucket
    for bucket in buckets:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {bucket}")
    cursor.close()



# Cria as tabelas no Trino para cada bucket
def create_tables(spark, minio_client, trino_connection, buckets, type_mapping):
    
    # Obtém o cursor do Trino para execução dos comandos SQL
    cursor = trino_connection.cursor()

    for bucket in buckets:

        print(f"Criando/atualizando as tabelas do schema {bucket}...")
        
        # Obtém a lista de tabelas do bucket
        cursor.execute(f'SHOW TABLES IN {bucket}')
        existing_tables = [row[0] for row in cursor.fetchall()]

        print(f"Tabelas existentes no schema {bucket}:")
        print(existing_tables)

        # Obtém a lista de tabelas do bucket
        tables = get_source_tables(minio_client, bucket)

        print(f"Tabelas a serem criadas/atualizadas no schema {bucket}:")
        print(tables)

        # Cria as tabelas no Trino
        for table, path in tables.items():

            print(f"Criando/atualizando a tabela {table}, cujo caminho é {path}...")

            # Define o Path do arquivo Delta Parquet
            delta_path = f"s3a://{bucket}/{path}"

            # Lê o arquivo Delta Parquet
            delta_table = DeltaTable.forPath(spark, delta_path).toDF()
            
            # Lê os metadados do arquivo Delta Parquet
            schema = delta_table.schema
            
            print(f"Colunas da tabela {table}: {schema.names}")

            # Aplica o mapeamento de tipos entre o Spark e o Trino
            for field in schema:
                field.dataType = type_mapping[str(field.dataType)]

            # Define as colunas da tabela para o comando SQL
            columns = ', '.join([f'{field.name} {field.dataType}' for field in schema])

            print(f"Colunas da tabela {table} no formato SQL: {columns}")

            # Verifica se a tabela já existe no Trino
            try:
                
                # Verifica se a tabela já existe no Trino
                if table in existing_tables:
                    
                    # Tabela já existe no Trino, então procede com a comparação e possível atualização
                    cursor.execute(f'SHOW COLUMNS FROM {bucket}.{table}')
                    existing_columns = [row[0] for row in cursor.fetchall()]

                    # Compara as colunas da tabela com as colunas existentes no Trino

                    if existing_columns != schema.names:
                        # Se as colunas forem diferentes, recria a tabela no Trino
                        cursor.execute(f"DROP TABLE IF EXISTS {bucket}.{table}")
                        create_table(cursor, bucket, table, delta_path, columns)

                        print(f"Tabela {table} atualizada com sucesso no schema {bucket}.")

                    else:
                        print(f"Tabela {table} já existe no schema {bucket}.")

                else:

                    # Tabela não existe no Trino, então cria a tabela
                    create_table(cursor, bucket, table, delta_path, columns)

                    print(f"Tabela {table} criada com sucesso no schema {bucket}.")


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
    create_schema(trino_connection, configuration['buckets'])

    # Cria as tabelas de cada bucket no Trino
    create_tables(spark, minio_client, trino_connection, configuration['buckets'], type_mapping)

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