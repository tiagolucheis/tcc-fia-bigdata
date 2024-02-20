# Define os imports necessários para a execução do código
from spark_scripts import create_context_functions as cfn
import pyspark.sql.functions as fn


# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'games': 'br_games'
}


# Aplica as transformações e regras de negócio específicas de cada tabela do IGDB
def apply_business_rules(spark, df, configuration):

    print(f"Aplicando as regras de negócio para a tabela {configuration['endpoint']} do {configuration['api_name']}.")
    print("Passo 1: Removendo as colunas especificadas.")
    # Remove as colunas especificadas
    df = cfn.remove_cols(df, configuration['cols_to_drop'])

    print("Passo 2: Convertendo as colunas de data para Unix Timestamp.")
    # Converte as colunas de data para Unix Timestamp
    df = cfn.convert_date_cols(df, configuration['date_cols'])

    print("Passo 3: Aplicando as regras de negócio específicas da tabela.")
    return eval(map_business_rules[configuration['endpoint']])(spark, df, configuration)





# ------------------------------- Games -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games do IGDB
def br_games(spark, df, configuration):
    
    # Altera o nome das colunas para o padrão do contexto
    df = (
        df
        .withColumnRenamed('comp_100', 'comp_time_100')
        .withColumnRenamed('comp_100_count', 'comp_time_100_count')
        .withColumnRenamed('comp_all', 'comp_time_all_styles')
        .withColumnRenamed('comp_all_count', 'comp_time_all_styles_count')
        .withColumnRenamed('comp_main', 'comp_time_main_story')
        .withColumnRenamed('comp_main_count', 'comp_time_main_story_count')
        .withColumnRenamed('comp_plus', 'comp_time_main_plus_sides')
        .withColumnRenamed('comp_plus_count', 'comp_time_main_plus_sides_count')
        .withColumnRenamed('count_comp', 'count_completed')
        .withColumnRenamed('game_name', 'name')
        .withColumnRenamed('profile_dev', 'developer')
        .withColumnRenamed('profile_platform', 'platform')
        .withColumnRenamed('profile_steam', 'steam_id')
        .withColumnRenamed('release_world', 'release_year')
    )

    return cfn.sort_cols(df)