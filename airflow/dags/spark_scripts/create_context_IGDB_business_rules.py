# Maperia as regras de negócio para cada tabela do IGDB
map_business_rules = {
    'game_modes': 'br_game_modes',
    'genres': 'br_genres',
    'platforms': 'br_platforms',
    'player_perspectives': 'br_player_perspectives'
}


# Aplica as transformações e regras de negócio específicas de cada tabela do IGDB
def apply_business_rules(df, configuration):

    # Remove as colunas especificadas
    df = remove_cols(df, configuration['cols_to_drop'])

    # Converte as colunas de data para Unix Timestamp
    df = convert_date_cols(df, configuration['date_cols'])

    return eval(map_business_rules[configuration['endpoint']])(df, configuration)



# ------------------------------- Funções Auxiliares -------------------------------

# Converte as colunas de data para Unix Timestamp
def convert_date_cols(df, date_cols):
    for col in date_cols:
        df = df.withColumn(col, fn.unix_timestamp(col, 'yyyy-MM-dd HH:mm:ss'))
    return df

# Remove as colunas especificadas do DataFrame
def remove_cols(df, cols):
    for col in cols:
        df = df.drop(col)
    return df



# ------------------------------- Game Modes -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela game_modes do IGDB
def br_game_modes(df, configuration):
    return df



# ------------------------------- Genres -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela genres do IGDB
def br_genres(df, configuration):
    return df



# ------------------------------- Platforms -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela platforms do IGDB
def br_platforms(df, configuration):

    # TO-DO: Enriquecer com DePara de Category
    # TO-DO: Especificar valor nulo para campos não preenchidos
    # TO-DO: Enriquecer com Platform Family, Logo e Version

    return df



# ------------------------------- Player Perspectives -------------------------------

# Aplica as transformações e regras de negócio específicas da tabela player_perspectives do IGDB
def br_player_perspectives(df, configuration):
    return df