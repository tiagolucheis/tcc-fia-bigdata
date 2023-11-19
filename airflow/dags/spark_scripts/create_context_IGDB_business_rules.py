# Define os imports necessários para a execução do código
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pyspark.sql.functions as fn

# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'collections': 'br_standard',
    'companies': 'br_standard',
    'franchises': 'br_standard',
    'game_engines': 'br_standard',
    'game_modes': 'br_standard',
    'games': 'br_games',
    'genres': 'br_standard',
    'involved_companies': 'br_standard',
    'keywords': 'br_standard',
    'language_supports': 'br_language_supports',
    'platforms': 'br_platforms',
    'player_perspectives': 'br_standard',
    'themes': 'br_standard',
    'multiplayer_modes': 'br_standard',
    'websites': 'br_websites'
}


# Aplica as transformações e regras de negócio específicas de cada tabela do IGDB
def apply_business_rules(spark, df, configuration):

    print(f"Aplicando as regras de negócio para a tabela {configuration['endpoint']} do {configuration['api_name']}.")
    print("Passo 1: Removendo as colunas especificadas.")
    # Remove as colunas especificadas
    df = remove_cols(df, configuration['cols_to_drop'])

    print("Passo 2: Convertendo as colunas de data para Unix Timestamp.")
    # Converte as colunas de data para Unix Timestamp
    df = convert_date_cols(df, configuration['date_cols'])

    print("Passo 3: Aplicando as regras de negócio específicas da tabela.")
    return eval(map_business_rules[configuration['endpoint']])(spark, df, configuration)



# ------------------------------- Funções Auxiliares -------------------------------

# Converte as colunas de data para Unix Timestamp
def convert_date_cols(df, date_cols):
    if date_cols:
        for col in date_cols:
            df = df.withColumn(col, fn.to_timestamp(fn.from_unixtime(col)))
    return df

# Remove as colunas especificadas do DataFrame
def remove_cols(df, cols):
    if cols:
        for col in cols:
            df = df.drop(col)
    return df

# Ordena as colunas de um DataFrame
def sort_cols(df):
    return df.select(sorted(df.columns))

# Obtém uma tabela de enumeração de um endpoint específico
def get_enum_table(spark, endpoint, table_name):
    delta_table_path = 's3a://raw/igdb_enums/' + endpoint + '/' + table_name + '/delta/'

    try:
        df_delta = DeltaTable.forPath(spark, delta_table_path).toDF()
    except AnalysisException:
        df_delta = None
        
    return df_delta

# Obtém uma tabela raw de um endpoint específico
def get_raw_table(spark, endpoint):
    delta_table_path = 's3a://raw/igdb/' + endpoint + '/delta/'

    try:
        df_delta = DeltaTable.forPath(spark, delta_table_path).toDF()
    except AnalysisException:
        df_delta = None
        
    return df_delta
    
# Enriquece a tabela de contexto com uma tabela de enumeração
def enrich_with_enum(spark, df, endpoint, enum_name):
    
    # Obtém a tabela de enumeração
    df_enum = get_enum_table(spark, endpoint, enum_name)
    # Se a tabela de enumeração existir
    if df_enum:
        
        col_name = enum_name + '_name'
        # Enriquece a tabela de contexto com a descrição da enumeração
        df = (
            df
            .join(df_enum.withColumnRenamed("name", col_name), df[enum_name] == df_enum.value, 'left')
            .drop(enum_name, "value")
            .withColumnRenamed(col_name, enum_name)
        )

    return df



# ------------------------------- Standard -------------------------------

# Aplica as transformações e regras de negócio padrão para as tabelas do IGDB
def br_standard(spark, df, configuration):
    return sort_cols(df)



# ------------------------------- Games -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games do IGDB
def br_games(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias do jogo
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'category')
    
    # Enriquece a tabela de contexto com o status do jogo
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'status')

    return sort_cols(df)




# ------------------------------- Platforms -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela platforms do IGDB
def br_platforms(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias da plataforma
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'category')
    
    # Obtém a tabela de família de plataformas
    df_family = (
        get_raw_table(spark, 'platform_families')
        .select(
            fn.col("id").alias("family_id"), 
            fn.col("name").alias("family_name")
            )
    )
    
    if df_family:
        # Enriquece a tabela de contexto com a família da plataforma
        df = (
            df
            .join(df_family, df.platform_family == df_family.family_id, 'left')
            .drop("family_id", "platform_family")
            .withColumnRenamed("family_name", "platform_family")
        )

    return sort_cols(df)



# ------------------------------- Language Supports -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela language_supports do IGDB
def br_language_supports(spark, df, configuration):
    
    # Obtém a tabela de linguagens
    df_languages = (
        get_raw_table(spark, 'languages')
        .select(
            fn.col("id").alias("language_id"), 
            fn.col("name").alias("language_name"),
            fn.col("locale").alias("locale")
            )
    )

    if df_languages:
        # Enriquece a tabela de contexto com a linguagem
        df = (
            df
            .join(df_languages, df.language == df_languages.language_id, 'left')
            .drop("language_id", "language")
            .withColumnRenamed("language_name", "language")
        )

    # Obtém a tabela de tipos de suporte de linguagem
    df_language_support_types = (
        get_raw_table(spark, 'language_support_types')
        .select(
            fn.col("id").alias("type_id"), 
            fn.col("name").alias("type_name")
            )
    )
    
    if df_language_support_types:
        # Enriquece a tabela de contexto com o tipo de suporte de linguagem
        df = (
            df
            .join(df_language_support_types, df.language_support_type == df_language_support_types.type_id, 'left')
            .drop("type_id", "language_support_type")
            .withColumnRenamed("type_name", "language_support_type")
        )

    return sort_cols(df)



# ------------------------------- Websites -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela websites do IGDB
def br_websites(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias de websites
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'category')

    return sort_cols(df)