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

    source_bucket_path = 's3a://context/'
    target_bucket_path = 's3a://trust/' + args.table_name + '/'

    # Cria uma lista que contém uma lista com o nome da API e o nome do endpoint para cada tabela de origem
    source_tables = [table.split('.') for table in args.source_tables.split()]
       
    configuration = {

        "table_name": args.table_name,                   # Nome da tabela
        "source_tables": source_tables,             # Tabelas utilizadas como fonte de dados (API e endpoint)

        "source_bucket_path": source_bucket_path,   # Caminho do bucket de origem
        "target_bucket_path": target_bucket_path,   # Caminho do bucket de destino
        
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
    df.write.format(format).mode(mode).option("overwriteSchema", "true").save(delta_table_path)



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
    
    # Obtém a data e hora da última criação da tablela trust
    df_target_control = get_target_control_table(spark, configuration["target_bucket_path"], configuration["schema"])
    last_target_run = get_last_run(df_target_control)
        
    # Obtém a data e hora da última atualização de cada tabela de origem
    last_source_updates = []

    for source_table in configuration['source_tables']:
        df_source_control = get_source_control_table(spark, configuration["source_bucket_path"] + source_table[0] + '/' + source_table[1] + '/')
        last_source_updates.append(get_last_run(df_source_control))
    
    # Verifica se houve atualização nas origenes desde a última geração da tabela trust
    for last_source_update in last_source_updates:
        if not last_target_run or (last_source_update["Exec_time"] > last_target_run["Exec_time"]):
            print("Houve atualização na origem desde a última geração da tabela.")
            return True
    
    print("Não houve atualização nas origens desde a última geração da tabela.")
    return False
    


# Ordena as colunas de um DataFrame
def sort_cols(df):
    return df.select(sorted(df.columns))