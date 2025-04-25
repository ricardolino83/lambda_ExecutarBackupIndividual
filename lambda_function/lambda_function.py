import json
import boto3
import pyodbc
import os
import datetime
import time
from botocore.exceptions import ClientError

# Variáveis Globais - Defina o driver ODBC apropriado para o Linux na imagem Docker
driver = '{ODBC Driver 18 for SQL Server}' # Certifique-se de que este driver está instalado no Dockerfile
s3_bucket_name = os.environ.get('S3_BACKUP_BUCKET', '!!!DEFINA_S3_BACKUP_BUCKET!!!') # Use variável de ambiente
rds_endpoint = os.environ.get('RDS_ENDPOINT', '!!!DEFINA_RDS_ENDPOINT!!!') # Obtenha via variável de ambiente
secret_name = os.environ.get('RDS_SECRET_NAME', '!!!DEFINA_RDS_SECRET_NAME!!!') # Obtenha via variável de ambiente
region_name = os.environ.get('AWS_REGION', 'sa-east-1') # Obtenha da execução do Lambda ou defina

global nome_do_banco_atual

# Configuração do Boto3 (reutiliza a sessão)
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=region_name)

def get_secret():
    """Busca o segredo do Secrets Manager."""
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except ClientError as e:
        print(f"Erro ao buscar segredo '{secret_name}': {e}")
        raise e
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o JSON do segredo '{secret_name}': {e}")
        raise e # Re-levanta a exceção para falhar a Lambda

def execute_sql_and_fetch_results(conn_str, script_content, fetch=True):
    """Conecta ao RDS, executa o script SQL e opcionalmente retorna os resultados."""
    conn = None
    cursor = None
    results = None # Inicializa como None
    try:
        conn = pyodbc.connect(conn_str, autocommit=True) # Autocommit pode ser bom para stored procedures como rds_backup
        cursor = conn.cursor()
        print(f"Executando SQL: {script_content[:100]}...") # Log truncado
        cursor.execute(script_content)

        if fetch:
            rows = cursor.fetchall()
            if rows:
                results = rows
                print(f"Consulta retornou {len(results)} linha(s).")
            else:
                print("Consulta não retornou linhas.")
        else:
             # Se for INSERT/UPDATE/DELETE ou EXEC sem resultado esperado, podemos verificar rowcount se aplicável
             print(f"Comando executado. Rowcount: {cursor.rowcount}")
             # Para EXEC msdb.dbo.rds_backup_database, esperamos o Task ID como resultado
             try:
                 rows = cursor.fetchall() # Tenta buscar mesmo se fetch=False, para pegar Task ID
                 if rows:
                     results = rows
                     print(f"Stored procedure retornou {len(results)} linha(s).")
                 else:
                      print("Stored procedure não retornou linhas.")
             except pyodbc.ProgrammingError as pe:
                 # Ignora erros do tipo "No results. Previous SQL was not a query."
                 if "No results" in str(pe):
                     print("Stored procedure não produziu um conjunto de resultados (esperado para status).")
                 else:
                     raise pe


        return results

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"\nERRO DE BANCO DE DADOS! SQLSTATE: {sqlstate}")
        print(f"Mensagem: {ex}")
        print(f"Script problemático (início): {script_content[:200]}...")
        return None # Retorna None em caso de erro de DB
    except Exception as e:
        print(f"\nOcorreu um erro inesperado na execução SQL: {e}")
        print(f"Script: {script_content[:200]}...")
        raise e # Re-levanta a exceção para falhar a Lambda
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def lambda_handler(event, context):
    nome_do_banco_atual = event['NomeBanco'] # Acessa o valor pela chave definida no Parameters
    all_backups_successful = False
    backup_successful = True
    backup_results = {}

    print("Iniciando execução da função de backup Lambda.")

    if not rds_endpoint:
        print("ERRO CRÍTICO: Variável de ambiente RDS_ENDPOINT não definida.")
        return {'statusCode': 500, 'body': json.dumps('RDS_ENDPOINT não configurado')}

    if not s3_bucket_name:
         print("ERRO CRÍTICO: Variável de ambiente S3_BACKUP_BUCKET não definida.")
         return {'statusCode': 500, 'body': json.dumps('S3_BACKUP_BUCKET não configurado')}

    print(f"Usando RDS Endpoint: {rds_endpoint}")
    print(f"Usando Bucket S3: {s3_bucket_name}")
    print(f"Buscando credenciais do Secret: {secret_name}")

    try:
        credentials_dict = get_secret()
        db_user = credentials_dict.get('username')
        db_password = credentials_dict.get('password')
    except Exception as e:
         print(f"Falha ao obter/processar credenciais do Secrets Manager: {e}")
         return {'statusCode': 500, 'body': json.dumps(f'Erro ao obter credenciais: {e}')}


    if not db_user or not db_password:
        print("ERRO CRÍTICO: Usuário ou senha não encontrados no segredo.")
        return {'statusCode': 500, 'body': json.dumps('Credenciais inválidas no segredo')}

    # Conexão inicial ao 'master' para listar bancos
    master_db_name = 'master'
    connection_string_master = (
        f'DRIVER={driver};'
        f'SERVER={rds_endpoint};'
        f'DATABASE={master_db_name};'
        f'UID={db_user};'
        f'PWD={db_password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=yes;'
        f'Timeout=30;' # Adicione esta linha
    )

    connection_string_backup = connection_string_master # Reutilizar a conexão master para os comandos msdb

    max_attempts = 3
    attempt = 0
    backup_successful = False
    task_id = None # Inicializa task_id

    while attempt < max_attempts and not backup_successful:
        attempt += 1
        try:
            # --- Iniciar o backup ---
            current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')
            backup_filename = f"{nome_do_banco_atual}-{current_timestamp}.bak"
            s3_backup_arn = f"arn:aws:s3:::{s3_bucket_name}/{backup_filename}"
            sql_script_backup = f"""
                            EXEC msdb.dbo.rds_backup_database
                                @source_db_name='{nome_do_banco_atual}',
                                @s3_arn_to_backup_to='{s3_backup_arn}',
                                @type='FULL';"""

            # A execução de rds_backup_database retorna o Task ID
            resultado_backup = execute_sql_and_fetch_results(connection_string_backup, sql_script_backup, fetch=True)

            if not resultado_backup or not resultado_backup[0] or not resultado_backup[0][0]:
                print(f"Erro ao iniciar o backup (não retornou Task ID) para {nome_do_banco_atual} na tentativa {attempt}.")
                task_id = None # Garante que não tentaremos verificar status sem task_id
                if attempt < max_attempts:
                    print("Aguardando 15s antes da próxima tentativa...")
                    time.sleep(15)
                continue # Pula para a próxima tentativa

            task_id = resultado_backup[0][0]
            print(f"Backup iniciado para {nome_do_banco_atual}. Task ID: {task_id}")

            # --- Verificar o status do backup ---
            status_check_attempts = 0
            max_status_checks = 60 # Limite de verificações (e.g., 60 * 15s = 15 minutos)

            while status_check_attempts < max_status_checks:
                status_check_attempts += 1
                print(f"Verificando status Task ID {task_id} (Tentativa {status_check_attempts}/{max_status_checks})...")
                time.sleep(15) # Aumentar o sleep entre verificações pode ser necessário

                sql_script_status = f"exec msdb.dbo.rds_task_status @task_id={task_id};"
                resultado_status = execute_sql_and_fetch_results(connection_string_backup, sql_script_status, fetch=True)
                status_backup = None
                
                if resultado_status and resultado_status[0] and len(resultado_status[0]) > 5:
                    lifecycle = resultado_status[0][3] # Índice 3 geralmente contém o lifecycle
                    status_backup = resultado_status[0][5] # Índice 5 geralmente contém o status
                    print(f"Status Task ID {task_id}: Lifecycle='{lifecycle}', Status='{status_backup}'")
                else:
                    print(f"Não foi possível obter o status completo para Task ID {task_id} nesta verificação. Tentando novamente...")
                    # Não incrementa status_check_attempts aqui, ou considera uma falha temporária
                    continue # Tenta buscar o status novamente na próxima iteração do loop de status

                if status_backup:
                    if status_backup == 'SUCCESS':
                        print(f"SUCESSO: Backup Task ID {task_id} para {nome_do_banco_atual} concluído.")
                        backup_successful = True
                        break # Sai do loop de verificação de status
                    elif status_backup == 'ERROR':
                        print(f"ERRO: Backup Task ID {task_id} para {nome_do_banco_atual} falhou.")
                        error_info = resultado_status[0][6] if len(resultado_status[0]) > 6 else "N/A" # Índice 6 pode conter informações de erro
                        print(f"Detalhes do erro RDS: {error_info}")
                        break # Sai do loop de verificação de status (falhou), vai tentar novamente o backup se houver tentativas
                    elif status_backup in ['CREATED', 'IN_PROGRESS']:
                        print(f"Backup Task ID {task_id} ainda está '{status_backup}'. Aguardando...")
                        # Continua no loop de verificação de status
                    elif status_backup in ['CANCEL_REQUESTED', 'CANCELLED']:
                        print(f"AVISO: Backup Task ID {task_id} foi '{status_backup}'.")
                        break # Sai do loop de verificação, considera falha para esta tentativa
                    else:
                        print(f"Status inesperado '{status_backup}' para Task ID {task_id}. Continuando verificação...")
                        # Continua verificando

            # Se saiu do loop de status sem sucesso
            if not backup_successful:
                print(f"Backup para {nome_do_banco_atual} (Task ID: {task_id}) não concluído com sucesso após {status_check_attempts} verificações.")
                # Se o loop de status esgotou, considera falha para a tentativa de backup atual
                if status_check_attempts >= max_status_checks:
                    print(f"Limite de verificação de status atingido para Task ID {task_id}.")

        except Exception as e:
            print(f"EXCEÇÃO GERAL durante a tentativa {attempt} para {nome_do_banco_atual}: {e}")
            # Loga a exceção, mas permite que o loop de tentativas continue

        # Se o backup falhou nesta tentativa (não está successful) E ainda há tentativas restantes
        if not backup_successful and attempt < max_attempts:
            print(f"Aguardando 30 segundos antes da próxima tentativa de backup para {nome_do_banco_atual}...")
            time.sleep(30)

    # Fim do loop de tentativas para um banco de dados
    if backup_successful:
        print(f"--- Backup de {nome_do_banco_atual} concluído com SUCESSO (Task ID: {task_id}) ---")
        backup_results[nome_do_banco_atual] = "SUCCESS"
        all_backups_successful = True # Marca que o backup deu certo
    else:
        print(f"--- FALHA FINAL no backup de {nome_do_banco_atual} após {max_attempts} tentativas ---")
        backup_results[nome_do_banco_atual] = "FAILED"
        all_backups_successful = False # Marca que pelo menos um falhou

    # Fim do loop por todos os bancos
    print("\n--- Resumo Final dos Backups ---")
    for db, status in backup_results.items():
        print(f"Banco: {db}, Status: {status}")

    if all_backups_successful:
        print("Backup concluído com sucesso.")
        return {'statusCode': 200, 'body': json.dumps(backup_results)}
    else:
        print("Backup falhou.")
        return {'statusCode': 200, 'body': json.dumps(backup_results)} # Ou 500 dependendo do requisito

# Bloco opcional para teste local (não será executado no Lambda)
if __name__ == "__main__":
    print("Executando localmente para teste...")
    # Simula o ambiente Lambda definindo variáveis de ambiente necessárias
    os.environ['RDS_ENDPOINT'] = '!!!DEFINA_S3_BACKUP_BUCKET!!!'
    os.environ['S3_BACKUP_BUCKET'] = '!!!DEFINA_RDS_ENDPOINT!!!'
    os.environ['RDS_SECRET_NAME'] = '!!!DEFINA_RDS_SECRET_NAME!!!'
    os.environ['AWS_REGION'] = 'sa-east-1'

    # Chama o handler
    result = lambda_handler(None, None)
    print("\nResultado da execução local:")
    print(result)
