# Define os imports necessários para a execução do código
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pyspark.sql.functions as fn



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
def get_configuration(args):

    source_bucket_path = 's3a://raw/' + args.api_name + '/' + args.endpoint + '/'
    target_bucket_path = 's3a://context/' + args.api_name + '/' + args.endpoint + '/'

    # Se não houver colunas de data ou colunas a serem removidas, define a lista como None
    date_cols = None
    cols_to_drop = None

    # Se houver colunas de data ou colunas a serem removidas, cria uma lista com os nomes das colunas
    if args.date_cols:
        date_cols = args.date_cols.split()
    if args.cols_to_drop:
        cols_to_drop = args.cols_to_drop.split()
    
    configuration = {
        "source_bucket_name": 'raw',                # Nome do bucket de origem
        "source_bucket_path": source_bucket_path,   # Caminho do bucket de origem
        "target_bucket_path": target_bucket_path,   # Caminho do bucket de destino

        "api_name": args.api_name,                  # Nome da API
        "endpoint": args.endpoint,                  # Nome do endpoint
        "date_cols": date_cols,                     # Colunas de data da tabela (para conversão para Unix Timestamp)
        "cols_to_drop": cols_to_drop,               # Colunas a serem removidas da tabela de contexto

        "schema": StructType([
            StructField("Exec_date", StringType(), nullable=False),
            StructField("Exec_time", LongType(), nullable=False),
            StructField("Table_Rows", StringType(), nullable=False),
            StructField("Total_duration", StringType(), nullable=False)
        ])
    }
    return configuration



# Obtém a tabela de controle de atualizações da origem de um endpoint específico 
def get_source_control_table(spark, source_bucket_path):
    return spark.read.parquet(source_bucket_path + '/control_table/')



# Obtém a tabela de controle de atualizações do destino de um endpoint específico (ou cria uma nova se ela não existir)
def get_target_control_table(spark, target_bucket_path, schema):
    try:
        return spark.read.parquet(target_bucket_path + '/control_table/')
        
    except AnalysisException:
        return spark.createDataFrame([], schema=schema)



# Atualiza a tabela de controle com a data e hora da execução, a quantidade de linhas da tabela e o tempo de execução
def update_target_control_table(spark, schema, target_bucket_path, exec_time, table_rows, formatted_time):
    # Cria um DataFrame com o novo registro da tabela de controle
    df_control = spark.createDataFrame([(
                                            exec_time.strftime("%Y-%m-%d %H:%M:%S"),
                                            int(exec_time.timestamp()),
                                            table_rows,
                                            formatted_time)], schema=schema)
    
    # Salva o novo registro da tabela de controle 
    (df_control
        .write
        .format('parquet')
        .mode('append')
        .save(target_bucket_path)
        )
    


# Salva o DataFrame como uma tabela Delta
def save_delta_table(df, delta_table_path, format="delta", mode="overwrite"):
    df.write.format(format).mode(mode).save(delta_table_path)
    


# Carrega o Dataframe com os dados atuais e define a data e hora da última leitura realizada
def get_delta_table(spark, delta_table_path):
    try:
        df_delta = DeltaTable.forPath(spark, delta_table_path).toDF()
    except AnalysisException:
        df_delta = None
        
    return df_delta



# Retorna a última execução de uma tabela com base na tabela de controle, em formato de timestamp
def get_last_run(df_control):
    return df_control.orderBy(fn.desc("Exec_time")).select("Exec_time").first()



# Verifica se houve atualização na origem desde a última geração do contexto para o endpoint
def check_source_update(spark, configuration):
    
    # Obtém a data e hora da última atualização do endpoint
    df_source_control = get_source_control_table(spark, configuration["source_bucket_path"])
    last_source_update = get_last_run(df_source_control)
    
    # Obtém a data e hora da última criação do contexto para o endpoint
    df_target_control = get_target_control_table(spark, configuration["target_bucket_path"], configuration["schema"])
    last_target_run = get_last_run(df_target_control)
    
    # Verifica se houve atualização na origem desde a última geração do contexto para o endpoint
    if not last_target_run or (last_source_update["Exec_time"] > last_target_run["Exec_time"]):
        print("Houve atualização na origem desde a última geração do contexto.")
        return True
    else:
        print("Não houve atualização na origem desde a última geração do contexto.")
        return False