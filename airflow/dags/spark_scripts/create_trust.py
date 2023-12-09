import sys
sys.path.append('/usr/local/airflow/dags/')

# Define os imports necessários para a execução do código
from spark_scripts import create_trust_functions as tfn
from spark_scripts import create_trust_business_rules as br
from datetime import datetime, timedelta, timezone
import time, argparse

# Define os argumentos do script
parser = argparse.ArgumentParser(description="Script para Criação/Atualização das tabelas trust.")

parser.add_argument("--table_name", type=str, required=True, help="Nome da tabela")
parser.add_argument("--source_tables", type=str, required=True, help="Tabelas utilizadas como fonte de dados, no formato 'api_name.table_name'")

args = parser.parse_args()



def main():
    # Obtém a sessão do Spark e as variáveis de configuração
    spark = tfn.create_spark_session()
    configuration = tfn.get_configuration(args)

    # Define o offset UTC para o Brasil (GMT-3)
    time_offset = timezone(timedelta(hours=-3))

    # Define a data e hora do início da execução para a tabela de controle de atualizações 
    import_time = datetime.now(time_offset)

    # Métrica de tempo de execução (início da execução)
    start_time = time.time()

    print(f"Iniciando a criação da tabela trust {configuration['table_name']}.")

    if tfn.check_source_update(spark, configuration) or 1==1: 
    # ATENÇÃO! Remover o 1==1 após os testes de desenvolvimento
        
        # Cria a tabela, aplicando transformações e regras de negócio específicas
        df_target = br.apply_business_rules(spark, configuration)

        # Salva a tabela de contexto no formato Delta
        tfn.save_delta_table(df_target, configuration["target_bucket_path"] + 'delta/')
        
        # Métrica de tempo de execução (fim da importação)
        end_time = time.time()
        execution_time = end_time - start_time

        # Calcula o tempo de execução
        hours, rem = divmod(execution_time, 3600)
        minutes, seconds = divmod(rem, 60)

        # Formata o tempo de execução
        formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        # Atualiza a tabela de controle de atualizações
        tfn.update_target_control_table(spark, configuration["schema"], configuration["target_bucket_path"] + 'control_table/', import_time, df_target.count(), formatted_time)

        print("Tabela atualizada com sucesso.")
        print(f"Processo finalizado em {formatted_time}.")   
        
    else:
        print("A tabela não requer atualização.")


if __name__ == "__main__":
    main()