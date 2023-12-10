import sys
sys.path.append('/usr/local/airflow/dags/')

from pyspark.sql.window import Window

# Define os imports necessários para a execução do código
from spark_scripts import create_trust_functions as tfn
import pyspark.sql.functions as fn


# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'games_by_platform': 'br_games_by_platform',
    'games_age_ratings': 'br_games_age_ratings',
    'game_modes_by_game': 'br_game_modes_by_game',
    'games_by_franchise': 'br_games_by_franchise',
    'games_by_genre': 'br_games_by_genre',
    'games_by_keyword': 'br_games_by_keyword',
    'games_by_themes': 'br_games_by_themes',
    'language_supports_by_game': 'br_language_supports_by_game',
    'player_perspectives_by_game': 'br_player_perspectives_by_game',
    'games': 'br_games',
}


# Aplica as transformações e regras de negócio específicas de cada tabela do IGDB
def apply_business_rules(spark, configuration):

    print(f"Aplicando as regras de negócio para a tabela {configuration['table_name']}.")
    return eval(map_business_rules[configuration['table_name']])(spark, configuration)





# ------------------------------- Games by Platform -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_by_platform
def br_games_by_platform(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_platforms = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')
    df_release_dates = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][2][0] + '/' + configuration['source_tables'][2][1] + '/delta/')

    # Separa os jogos cuja plataforma é nula, para uní-los aos jogos que possuem plataforma
    df_games_null_platform = (
        df_games
        .filter(fn.col('platforms').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('platform_id')
        )
    )

    # Explode a coluna platforms para obter uma linha para cada plataforma
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('platforms')).alias('platform_id')
        )
    )

    # Une os jogos cuja plataforma é nula aos jogos que possuem plataforma
    df = df.union(df_games_null_platform)

    # Enriquece a tabela com o nome da plataforma
    df = (
        df
        .join(df_platforms, df.platform_id == df_platforms.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.platform_id,
            fn.col('name').alias('platform_name'),
            fn.col('generation').alias('platform_generation'),
            fn.col('platform_family').alias('platform_family'),
            fn.col('category').alias('platform_category')
        )
    )

    # Identifica a menor data e ano de lançamento para cada jogo e plataforma
    df_release_years = (
        df_release_dates
        .select(
            fn.col("game").alias("game_id"),
            fn.col("platform_id"),
            fn.col('region'),
            fn.col('date'),
            fn.col('y')
        )
        .groupBy("game_id", "platform_id")
        .agg(
            fn.min(fn.col("date")).alias('release_date'),
            fn.min(fn.col("y")).alias('release_year')
        )
        .orderBy("game_id", "platform_id")
    )

    # Enriquece a tabela com o ano de lançamento
    df = (
        df
        .join(df_release_years, ['game_id', 'platform_id'], 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.platform_id,
            df.platform_name,
            df.platform_generation,
            df.platform_family,
            df.platform_category,
            df_release_years.release_date,
            df_release_years.release_year
        )
        .orderBy("game_id", "platform_id")
    )

    # Categoriza a família de plataformas de acordo com critérios pré-definidos, apenas para as plataformas que não possuem família definida
    df = (
        df
        .withColumn('platform_family', fn.when(fn.col('platform_family').isNull(), 
                                                fn.when(fn.col('platform_category').isin('arcade'), 'Arcade').otherwise(
                                                fn.when(fn.col('platform_category').isin('console'), 'Other Consoles').otherwise(
                                                fn.when(fn.col('platform_category').isin('portable_console'), 'Portable Console').otherwise(
                                                fn.when(fn.col('platform_category').isin('platform'), 'Cloud Gaming').otherwise(
                                                fn.when(fn.col('platform_category').isin('computer'), 'Computer').otherwise(
                                                fn.when(fn.col('platform_category').isin('operating_system'),
                                                        fn.when(fn.col('platform_name').isin('iOS', 'Android', 'BlackBerry OS', 'Windows Mobile', 'Palm OS', 'Windows Phone'), 'Mobile')
                                                        .otherwise('Computer')
                                                )))))))
                                                .otherwise(fn.col('platform_family')))
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('platform_name', fn.coalesce(fn.col('platform_name'), fn.lit('(Não Definida)')))
        .withColumn('platform_category', fn.coalesce(fn.col('platform_category'), fn.lit('(Não Definida)')))
        .withColumn('platform_family', fn.coalesce(fn.col('platform_family'), fn.lit('(Não Definida)')))
        .withColumn('platform_generation', fn.coalesce(fn.col('platform_generation'), fn.lit('(Não Definida)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Games Age Ratings -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_age_ratings
def br_games_age_ratings(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_age_ratings = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cuja classificação etária é nula, para uní-los aos jogos que possuem classificação etária
    df_games_null_age_rating = (
        df_games
        .filter(fn.col('age_ratings').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('age_rating_id')
        )
    )

    # Explode a coluna age_ratings para obter uma linha para cada classificação etária
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('age_ratings')).alias('age_rating_id')
        )
    )

    # Une os jogos cuja classificação etária é nula aos jogos que possuem classificação etária
    df = df.union(df_games_null_age_rating)

    # Enriquece a tabela com a classificação etária
    df = (
        df
        .join(df_age_ratings, df.age_rating_id == df_age_ratings.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.age_rating_id,
            fn.col('category').alias('age_rating_category'),
            fn.col('rating').alias('age_rating')
        )
        .orderBy("game_id", "age_rating_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('age_rating', fn.coalesce(fn.col('age_rating'), fn.lit('(Não Definida)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Game Modes by Game -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela game_modes_by_game
def br_game_modes_by_game(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_game_modes = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cujo modo de jogo é nulo, para uní-los aos jogos que possuem modo de jogo
    df_games_null_game_mode = (
        df_games
        .filter(fn.col('game_modes').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('game_mode_id')
        )
    )

    # Explode a coluna game_modes para obter uma linha para cada modo de jogo
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('game_modes')).alias('game_mode_id')
        )
    )

    # Une os jogos cujo modo de jogo é nulo aos jogos que possuem modo de jogo
    df = df.union(df_games_null_game_mode)

    # Enriquece a tabela com o modo de jogo
    df = (
        df
        .join(df_game_modes, df.game_mode_id == df_game_modes.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.game_mode_id,
            fn.col('name').alias('game_mode_name')
        )
        .orderBy("game_id", "game_mode_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('game_mode_name', fn.coalesce(fn.col('game_mode_name'), fn.lit('(Não Definido)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Games by Franchise -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_by_franchise
def br_games_by_franchise(spark, configuration):

    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_franchises = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cuja franquia é nula, para uní-los aos jogos que possuem franquia
    df_games_null_franchise = (
        df_games
        .filter(fn.col('franchises').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.year((fn.col('first_release_date'))).alias('release_year'),
            fn.lit(None).alias('franchise_id')
        )
    )

    # Explode a coluna franchises para obter uma linha para cada franquia
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.year((fn.col('first_release_date'))).alias('release_year'),
            fn.explode(fn.col('franchises')).alias('franchise_id')
        )
    )

    # Une os jogos cuja franquia é nula aos jogos que possuem franquia
    df = df.union(df_games_null_franchise)

    # Enriquece a tabela com a franquia
    df = (
        df
        .join(df_franchises, df.franchise_id == df_franchises.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.franchise_id,
            fn.col('name').alias('franchise_name'),
            df.release_year
        )
        .orderBy("game_id", "franchise_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('franchise_name', fn.coalesce(fn.col('franchise_name'), fn.lit('(Sem franquia)')))
    )

    return tfn.sort_cols(df)


# ------------------------------- Games by Genre -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_by_genre
def br_games_by_genre(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_genres = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cujo gênero é nulo, para uní-los aos jogos que possuem gênero
    df_games_null_genre = (
        df_games
        .filter(fn.col('genres').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('genre_id')
        )
    )

    # Explode a coluna genres para obter uma linha para cada gênero
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('genres')).alias('genre_id')
        )
    )

    # Une os jogos cujo gênero é nulo aos jogos que possuem gênero
    df = df.union(df_games_null_genre)

    # Enriquece a tabela com o gênero
    df = (
        df
        .join(df_genres, df.genre_id == df_genres.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.genre_id,
            fn.col('name').alias('genre_name')
        )
        .orderBy("game_id", "genre_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('genre_name', fn.coalesce(fn.col('genre_name'), fn.lit('(Não Definido)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Games by Keyword -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_by_keyword
def br_games_by_keyword(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_keywords = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cuja palavra-chave é nula, para uní-los aos jogos que possuem palavra-chave
    df_games_null_keyword = (
        df_games
        .filter(fn.col('keywords').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('keyword_id')
        )
    )

    # Explode a coluna keywords para obter uma linha para cada palavra-chave
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('keywords')).alias('keyword_id')
        )
    )

    # Une os jogos cuja palavra-chave é nula aos jogos que possuem palavra-chave
    df = df.union(df_games_null_keyword)

    # Enriquece a tabela com a palavra-chave
    df = (
        df
        .join(df_keywords, df.keyword_id == df_keywords.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.keyword_id,
            fn.col('name').alias('keyword_name')
        )
        .orderBy("game_id", "keyword_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('keyword_name', fn.coalesce(fn.col('keyword_name'), fn.lit('(Sem palavra-chave)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Games by Theme -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games_by_theme
def br_games_by_themes(spark, configuration):
    
    # Obtém as tabelas de origem
    df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_themes = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')

    # Separa os jogos cujo tema é nulo, para uní-los aos jogos que possuem tema
    df_games_null_theme = (
        df_games
        .filter(fn.col('themes').isNull())
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.lit(None).alias('theme_id')
        )
    )

    # Explode a coluna themes para obter uma linha para cada tema
    df = (
        df_games
        .select(
            fn.col('id').alias('game_id'),
            fn.col('name').alias('game_name'),
            fn.col('category').alias('game_category'),
            fn.explode(fn.col('themes')).alias('theme_id')
        )
    )

    # Une os jogos cujo tema é nulo aos jogos que possuem tema
    df = df.union(df_games_null_theme)

    # Enriquece a tabela com o tema
    df = (
        df
        .join(df_themes, df.theme_id == df_themes.id, 'left_outer')
        .select(
            df.game_id,
            df.game_name,
            df.game_category,
            df.theme_id,
            fn.col('name').alias('theme_name')
        )
        .orderBy("game_id", "theme_id")
    )

    # Define valores padrão para os campos nulos
    df = (
        df
        .withColumn('theme_name', fn.coalesce(fn.col('theme_name'), fn.lit('(Não Definido)')))
    )

    return tfn.sort_cols(df)



# ------------------------------- Language Supports by Game -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela language_supports_by_game
def br_language_supports_by_game(spark, configuration):
        
        # Obtém as tabelas de origem
        df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
        df_languages = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')
        
        # Separa os jogos cujo suporte a idiomas é nulo, para uní-los aos jogos que possuem suporte a idiomas
        df_games_null_language_support = (
            df_games
            .filter(fn.col('language_supports').isNull())
            .select(
                fn.col('id').alias('game_id'),
                fn.col('name').alias('game_name'),
                fn.col('category').alias('game_category'),
                fn.lit(None).alias('language_id')
            )
        )
    
        # Explode a coluna language_support para obter uma linha para cada idioma
        df = (
            df_games
            .select(
                fn.col('id').alias('game_id'),
                fn.col('name').alias('game_name'),
                fn.col('category').alias('game_category'),
                fn.explode(fn.col('language_supports')).alias('language_id')
            )
        )
    
        # Une os jogos cujo suporte a idiomas é nulo aos jogos que possuem suporte a idiomas
        df = df.union(df_games_null_language_support)

        # Enriquece a tabela com o idioma
        df = (
            df
            .join(df_languages, df.language_id == df_languages.id, 'left_outer')
            .select(
                df.game_id,
                df.game_name,
                df.game_category,
                df.language_id,
                fn.col('language'),
                fn.col('language_support_type'),
                fn.col('locale')
            )
            .orderBy("game_id", "language_id")
        )
    
        # Define valores padrão para os campos nulos
        df = (
            df
            .withColumn('language', fn.coalesce(fn.col('language'), fn.lit('(Não Definido)')))
        )

        return tfn.sort_cols(df)



# ------------------------------- Player Perspectives by Game -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela player_perspectives_by_game
def br_player_perspectives_by_game(spark, configuration):
        
        # Obtém as tabelas de origem
        df_games = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
        df_player_perspectives = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')
    
        # Separa os jogos cuja perspectiva do jogador é nula, para uní-los aos jogos que possuem perspectiva do jogador
        df_games_null_player_perspective = (
            df_games
            .filter(fn.col('player_perspectives').isNull())
            .select(
                fn.col('id').alias('game_id'),
                fn.col('name').alias('game_name'),
                fn.col('category').alias('game_category'),
                fn.lit(None).alias('player_perspective_id')
            )
        )
    
        # Explode a coluna player_perspectives para obter uma linha para cada perspectiva do jogador
        df = (
            df_games
            .select(
                fn.col('id').alias('game_id'),
                fn.col('name').alias('game_name'),
                fn.col('category').alias('game_category'),
                fn.explode(fn.col('player_perspectives')).alias('player_perspective_id')
            )
        )
    
        # Une os jogos cuja perspectiva do jogador é nula aos jogos que possuem perspectiva do jogador
        df = df.union(df_games_null_player_perspective)
    
        # Enriquece a tabela com a perspectiva do jogador
        df = (
            df
            .join(df_player_perspectives, df.player_perspective_id == df_player_perspectives.id, 'left_outer')
            .select(
                df.game_id,
                df.game_name,
                df.game_category,
                df.player_perspective_id,
                fn.col('name').alias('player_perspective_name')
            )
            .orderBy("game_id", "player_perspective_id")
        )
    
        # Define valores padrão para os campos nulos
        df = (
            df
            .withColumn('player_perspective_name', fn.coalesce(fn.col('player_perspective_name'), fn.lit('(Não Definido)')))
        )
    
        return tfn.sort_cols(df)


# ------------------------------- Games -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela games e enrique com os dados das tabelas de contexto do HLTB
def br_games(spark, configuration):

    # Obtém as tabelas de origem
    df_games_igdb = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][0][0] + '/' + configuration['source_tables'][0][1] + '/delta/')
    df_games_hltb = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][1][0] + '/' + configuration['source_tables'][1][1] + '/delta/')
    df_covers = tfn.get_delta_table(spark, configuration['source_bucket_path'] + configuration['source_tables'][2][0] + '/' + configuration['source_tables'][2][1] + '/delta/')

    # Filtra os campos necessários para a tabela final de jogos
    df = (
        df_games_igdb
        .select(
            fn.col('aggregated_rating').alias('rating_igdb_external_critics'),
            fn.col('aggregated_rating_count').alias('rating_igdb_external_critics_count'),
            fn.col('bundles'),
            fn.col('category'),
            fn.col('collection'),
            fn.col('cover').alias('cover_id'),
            fn.col('dlcs'),
            fn.col('expanded_games'),
            fn.col('expansions'),
            fn.col('first_release_date'),
            fn.col('follows'),
            fn.col('forks'),
            fn.col('franchise'),
            fn.col('game_engines'),
            fn.col('hypes').alias('follows_before_release'),
            fn.col('id'),
            fn.col('name'),
            fn.col('parent_game'),
            fn.col('ports'),
            fn.col('rating').alias('rating_igdb_users'),
            fn.col('rating_count').alias('rating_igdb_users_count'),
            fn.col('remakes'),
            fn.col('remasters'),
            fn.col('similar_games'),
            fn.col('standalone_expansions'),
            fn.col('status'),
            fn.col('total_rating').alias('rating_igdb_total'),
            fn.col('total_rating_count').alias('rating_igdb_total_count'),
            fn.col('version_parent'),
            fn.col('version_title')
        )
    )

    # Enriquece a tabela com a url da capa do jogo
    df_covers = df_covers.select(fn.col('id').alias('id_cover'), fn.col('url'))

    df = (
        df
        .join(df_covers, df.cover_id == df_covers.id_cover, 'left_outer')
        # Renomeia a coluna url para cover_url
        .withColumnRenamed('url', 'cover_url')
        .drop('id_cover')
    )

    # Filtra as colunas da tabela HLTB para facilitar o join com o IGDB
    df_games_hltb = (
        df_games_hltb
        # Remove as colunas desnecessárias
        .drop(
            'extracted_datetime',
            'game_alias',
            'game_type',
            'id',
            'name',
            'platform',
            'release_year',
            'steam_id'
        )
        .withColumnRenamed('review_score', 'rating_hltb_users')
    )


    # Enriquece a tabela com os dados do HLTB
    df = (
        df
        .join(df_games_hltb, df.id == df_games_hltb.game_id, 'left_outer')
        .drop('game_id')
        .orderBy('id')
    )

    # Cria um campo para calcular a avaliação média dos usuários e total
    df = (
        df
        .withColumn('average_rating_users', (fn.coalesce(fn.col('rating_igdb_users'), fn.col('rating_hltb_users')) + fn.coalesce(fn.col('rating_hltb_users'), fn.col('rating_igdb_users'))) / 2)
        .withColumn('average_rating_total', (fn.col('rating_igdb_total') + fn.coalesce(fn.col('rating_hltb_users'), fn.col('rating_igdb_total'))) / 2)
    )

    return tfn.sort_cols(df)