# Define os imports necessários para a execução do código
import spark_scripts.create_context_functions as cfn
import spark_scripts.create_context_IGDB_business_rules as br
from datetime import datetime, timedelta, timezone
import time, argparse

# Define os argumentos do script
parser = argparse.ArgumentParser(description="Script para Criação/Atualização das tabelas de contexto do IGDB.")

parser.add_argument("--api_name", type=str, required=True, help="Nome da API")
parser.add_argument("--endpoints", type=str, required=True, help="Lista de Endpoints")
parser.add_argument("--date_cols", type=str, required=True, help="Coluna de controle de atualização do dado")
parser.add_argument("--cols_to_drop", type=str, required=True, help="Colunas a serem removidas da tabela de contexto")

args = parser.parse_args()



def main():
    # Obtém a sessão do Spark e as variáveis de configuração
    spark = cfn.create_spark_session()
    configuration = cfn.get_configuration(args)

    # Define o offset UTC para o Brasil (GMT-3)
    time_offset = timezone(timedelta(hours=-3))

    # Define a data e hora do início da execução para a tabela de controle de atualizações 
    import_time = datetime.now(time_offset)

    # Métrica de tempo de execução (início da execução)
    start_time = time.time()

    print(f"Iniciando a criação da tabela de contexto para a tabela {configuration['endpoint']} do {configuration['api_name']}.")

    if cfn.check_source_update(spark, configuration):
        
        # Obtém a tabela delta raw correspondente
        df_source = cfn.get_delta_table(spark, configuration["source_bucket_path"] + 'delta/')
        
        # Aplica as transformações e regras de negócio específicas da tabela
        df_source = br.apply_business_rules(df_source, configuration)

        # Salva a tabela de contexto no formato Delta
        cfn.save_delta_table(df_source.toDF(), configuration["target_bucket_path"] + 'delta/')
        
        # Métrica de tempo de execução (fim da importação)
        end_time = time.time()
        execution_time = end_time - start_time

        # Calcula o tempo de execução
        hours, rem = divmod(execution_time, 3600)
        minutes, seconds = divmod(rem, 60)

        # Formata o tempo de execução
        formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        # Atualiza a tabela de controle de atualizações
        cfn.update_target_control_table(spark, configuration["schema"], configuration["target_bucket_path"] + 'control_table/', import_time, df_source.count(), formatted_time)

        print("Tabela de contexto atualizada com sucesso.")
        print(f"Processo finalizado em {formatted_time}.")   
        
    else:
        print("A tabela de contexto não requer atualização.")


if __name__ == "__main__":
    main()