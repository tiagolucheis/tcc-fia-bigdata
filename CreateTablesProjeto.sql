CREATE SCHEMA IF NOT EXISTS projeto2;


CREATE TABLE IF NOT EXISTS projeto2.pokemon ( 
    _id BIGINT, 
    altura int, 
    experiencia int, 
    formas ARRAY(
                 ROW(
                     name varchar, 
                      url varchar)
                ), 
    nome VARCHAR, 
    peso int, 
    type varchar
) 
WITH ( external_location = 's3a://raw/mongodb/pokemon', format = 'PARQUET' );


CREATE TABLE IF NOT EXISTS projeto2.type ( 
  _id varchar, 
  moves ARRAY(
              ROW(
                  name varchar, 
                  url varchar
                  )
              )
) 
WITH ( external_location = 's3a://raw/mongodb/type', format = 'PARQUET' );


CREATE TABLE IF NOT EXISTS projeto2.pokemons_moves ( 
    id BIGINT, 
    nome VARCHAR,
    move VARCHAR,
    type VARCHAR)
WITH ( external_location = 's3a://context/projeto/pokemons_moves_unpart', format = 'PARQUET');


CREATE TABLE IF NOT EXISTS projeto2.count_name ( 
    nome varchar, 
    count_move int
) 
WITH ( external_location = 's3a://trust/projeto/count_name', format = 'PARQUET' );


CREATE TABLE IF NOT EXISTS projeto2.count_type ( 
    type varchar, 
    count_move int
) 
WITH ( external_location = 's3a://trust/projeto/count_type', format = 'PARQUET' );