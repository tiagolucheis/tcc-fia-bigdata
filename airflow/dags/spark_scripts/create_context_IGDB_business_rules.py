# Define os imports necessários para a execução do código
from spark_scripts import create_context_functions as cfn
import pyspark.sql.functions as fn


# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'age_ratings': 'br_age_ratings',
    'age_rating_content_descriptions': 'br_categories',
    'collections': 'br_standard',
    'companies': 'br_companies',
    'covers': 'br_covers',
    'external_games': 'br_categories',
    'franchises': 'br_standard',
    'game_engines': 'br_standard',
    'game_localizations': 'br_game_localizations',
    'game_modes': 'br_standard',
    'games': 'br_games',
    'genres': 'br_standard',
    'involved_companies': 'br_standard',
    'keywords': 'br_standard',
    'language_supports': 'br_language_supports',
    'multiplayer_modes': 'br_standard',
    'platforms': 'br_platforms',
    'player_perspectives': 'br_standard',
    'release_dates': 'br_release_dates',
    'themes': 'br_standard',
    'websites': 'br_categories'
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




# ------------------------------- Standard -------------------------------

# Aplica as transformações e regras de negócio padrão para as tabelas do IGDB
def br_standard(spark, df, configuration):
    return cfn.sort_cols(df)



# ------------------------------- Categories -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela categories do IGDB
def br_categories(spark, df, configuration):

    # Enriquece a tabela de contexto com as categorias definidas na tabela de enumeração
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'category')

    return cfn.sort_cols(df)



# ------------------------------- Companies -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela companies do IGDB
def br_companies(spark, df, configuration):

    # Duplica a coluna 'country' para criar as colunas 'Country Code' e 'Country Alpha-3'
    df = (
        df
        .withColumn("countryname", fn.col("country"))
        .withColumn("countryalpha3", fn.col("country"))
        .withColumnRenamed("country", "coutry_code")
    )

    # Enriquece a tabela de contexto com o país da companhia, a partir da tabela de enumeração
    df = cfn.enrich_with_enum(spark, df, "aux_enums",'iso_country_code', 'countryname')

    # Enriquece a tabela de contexto com o código Alpha-3 do país da companhia, a partir da tabela de enumeração
    df = cfn.enrich_with_enum(spark, df, "aux_enums",'iso_country_code', 'countryalpha3')

    #Renomeia as colunas
    df = (
        df
        .withColumnRenamed("countryname", "country_name")
        .withColumnRenamed("countryalpha3", "country_alpha3")
    )

    return cfn.sort_cols(df)


# ------------------------------- Covers -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela covers do IGDB
def br_covers(spark, df, configuration):

    # Altera a url para obter a imagem em alta resolução
    df = (
        df
        .withColumn("url", fn.regexp_replace(fn.col("url"), "t_thumb", "t_cover_big"))
    )

    return cfn.sort_cols(df)


# ------------------------------- Games -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games do IGDB
def br_games(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias do jogo
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'category')
    
    # Enriquece a tabela de contexto com o status do jogo
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'status')

    return cfn.sort_cols(df)




# ------------------------------- Platforms -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela platforms do IGDB
def br_platforms(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias da plataforma
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'category')
    
    # Obtém a tabela de família de plataformas
    df_family = (
        cfn.get_raw_table(spark, 'platform_families')
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

    return cfn.sort_cols(df)



# ------------------------------- Language Supports -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela language_supports do IGDB
def br_language_supports(spark, df, configuration):
    
    # Obtém a tabela de linguagens
    df_languages = (
        cfn.get_raw_table(spark, 'languages')
        .select(
            fn.col("id").alias("language_id"), 
            fn.col("name").alias("language_name"),
            fn.col("locale")
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
        cfn.get_raw_table(spark, 'language_support_types')
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

    return cfn.sort_cols(df)



# ------------------------------- Age Ratings -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela age_ratings do IGDB
def br_age_ratings(spark, df, configuration):
    
    # Enriquece a tabela de contexto com as categorias do rating
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'category')

    # Enriquece a tabela de contexto com o tipo de rating
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'rating')
    
    return cfn.sort_cols(df)




# ------------------------------- Game Localizations -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela game_localizations do IGDB
def br_game_localizations(spark, df, configuration):
    
    # Obtém a tabela de regiões
    df_regions = (
        cfn.get_raw_table(spark, 'regions')
        .select(
            fn.col("id").alias("region_id"), 
            fn.col("name").alias("region_name")
            )
    )

    if df_regions:
        # Enriquece a tabela de contexto com a região
        df = (
            df
            .join(df_regions, df.region == df_regions.region_id, 'left')
            .drop("region_id", "region")
            .withColumnRenamed("region_name", "region")
        )

    return cfn.sort_cols(df)



# ------------------------------- Release Dates -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela release_dates do IGDB
def br_release_dates(spark, df, configuration):
    
    # Enriquece a tabela de contexto com a categoria de data de lançamento
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'category')

    # Enriquece a tabela de contexto com a região do lançamento
    df = cfn.enrich_with_enum(spark, df, "igdb_enums",configuration["endpoint"], 'region')

    # Obtém a tabela de status de lançamento
    df_status = (
        cfn.get_raw_table(spark, 'release_date_statuses')
        .select(
            fn.col("id").alias("status_id"), 
            fn.col("name").alias("status_name")
            )
    )

    if df_status:
        # Enriquece a tabela de contexto com o status do lançamento
        df = (
            df
            .join(df_status, df.status == df_status.status_id, 'left')
            .drop("status_id", "status")
            .withColumnRenamed("status_name", "status")
        )

    # Obtém a tabela de plataformas
    df_platforms = (
        cfn.get_raw_table(spark, 'platforms')
        .select(
            fn.col("id").alias("platform_id"), 
            fn.col("name").alias("platform_name")
            )
    )

    if df_platforms:
        # Enriquece a tabela de contexto com a plataforma
        df = (
            df
            .join(df_platforms, df.platform == df_platforms.platform_id, 'left')
            .drop("platform")
        )
    
    return cfn.sort_cols(df)