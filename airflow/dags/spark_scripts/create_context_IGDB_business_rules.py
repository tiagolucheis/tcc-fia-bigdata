# Define os imports necessários para a execução do código
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pyspark.sql.functions as fn

# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'game_modes': 'br_game_modes',
    'games': 'br_games',
    'genres': 'br_genres',
    'platforms': 'br_platforms',
    'player_perspectives': 'br_player_perspectives'
}


# Aplica as transformações e regras de negócio específicas de cada tabela do IGDB
def apply_business_rules(spark, df, configuration):

    # Remove as colunas especificadas
    df = remove_cols(df, configuration['cols_to_drop'])

    # Converte as colunas de data para Unix Timestamp
    df = convert_date_cols(df, configuration['date_cols'])

    return eval(map_business_rules[configuration['endpoint']])(spark, df, configuration)



# ------------------------------- Funções Auxiliares -------------------------------

# Converte as colunas de data para Unix Timestamp
def convert_date_cols(df, date_cols):
    for col in date_cols:
        df = df.withColumn(col, fn.to_timestamp(fn.from_unixtime(col)))
    return df

# Remove as colunas especificadas do DataFrame
def remove_cols(df, cols):
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



# ------------------------------- Game Modes -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela game_modes do IGDB
def br_game_modes(spark, df, configuration):
    return df


# ------------------------------- Games -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games do IGDB
def br_games(spark, df, configuration):
    
    # Guarda as colunas originais do DataFrame para selecionar após as transformações
    original_cols = df.columns

    # Enriquece a tabela de contexto com as categorias do jogo
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'category')
    
    # Enriquece a tabela de contexto com o status do jogo
    df = enrich_with_enum(spark, df, configuration["endpoint"], 'status')

    # TO-DO: Especificar valor nulo para campos não preenchidos e outras regras

    return sort_cols(df)



# ------------------------------- Genres -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela genres do IGDB
def br_genres(spark, df, configuration):
    return df



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

    # TO-DO: Especificar valor nulo para campos não preenchidos

    return sort_cols(df)



# ------------------------------- Player Perspectives -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela player_perspectives do IGDB
def br_player_perspectives(spark, df, configuration):
    return df