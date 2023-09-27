# Define os imports necessários para a execução do código
import pyspark.sql.functions as fn
from howlongtobeatpy import HowLongToBeat
import aiohttp, asyncio
from pyspark.sql.types import *


# Função para obter os dados do site How Long To Beat para um jogo, a partir de seu nome
async def get_game_data_by_name(game, queue, semaphore):
    
    async with semaphore: 
        hltb = HowLongToBeat(0.01)  # Pequeno atraso entre as chamadas para não sobrecarregar a API
        
        results_list = await hltb.async_search(game['name'])
        
        best_element = None

        if results_list is not None and len(results_list) > 0:

            max_similarity = max(results_list, key=lambda element: element.similarity).similarity

            if (max_similarity > 0.8):
                
                # Filtrar elementos com a maior similaridade
                best_elements = [r for r in results_list if r.similarity == max_similarity]

                if (len(best_elements) > 1):
                    # Verifique o ano apenas nos elementos com a maior similaridade
                    for r in best_elements:
                        if r.release_world == game['release_year']:
                            best_element = r
                            # print(f"Jogo: {game.name}, Ano: {game.release_year}, Resultado: {best_element.game_name}, Ano: {best_element.release_world}, Similaridade: {best_element.similarity}")
                            break  # Encontramos uma correspondência exata, então podemos sair do loop
                else:
                    best_element = best_elements[0]

        await queue.put((game, best_element))

        return game, best_element 

# Função para obter os dados do site How Long To Beat para um jogo, a partir de seu ID
async def get_game_data_by_id(game, queue, semaphore):
    
    async with semaphore: 
        hltb = HowLongToBeat(0.01)  # Pequeno atraso entre as chamadas para não sobrecarregar a API
        
        best_element = await hltb.async_search_from_id(game['game_id'])
        await queue.put((game, best_element))

        return game, best_element 


# Função para processar os resultados
async def process_results(spark, queue, extraction_timestamp):
    
    games_found = 0
    games_not_found = 0

    df_json = spark.createDataFrame([], StructType([]))

    while not queue.empty():
    
        game, result = await queue.get()
        
        if result is None:
            #print(f"Jogo: {game.name}, Resultado: Não encontrado")
            games_not_found += 1
        else:
            #print(f"Jogo: {game.name}, Resultado: {result.game_name}")
            games_found += 1
            
            df = result.json_content
            df['id'] = game['id']
            df['extracted_datetime'] = extraction_timestamp           

            df_json = df_json.unionByName(spark.createDataFrame([df]), allowMissingColumns = True)
        
    print(f"Jogos encontrados: {games_found}")
    print(f"Jogos não encontrados: {games_not_found}")

    return df_json

# Função para executar o loop de extração dos dados de cada jogo, de forma assíncrona e concorrente
async def loop_hltb(search_type, spark, df_games, queue, max_concurrent_coroutines, extraction_timestamp):
    
    semaphore = asyncio.Semaphore(max_concurrent_coroutines)
    
    async with aiohttp.ClientSession() as session:
        
        if search_type == 'ID':
            tasks = [get_game_data_by_id(game.asDict(), queue, semaphore) for game in df_games]
        elif search_type == 'GN':
            tasks = [get_game_data_by_name(game.asDict(), queue, semaphore) for game in df_games]
        
        await asyncio.gather(*tasks)
        extracted_data = await process_results(spark, queue, extraction_timestamp)
        
    return extracted_data
    
