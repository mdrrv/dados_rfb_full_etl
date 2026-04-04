import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# Carrega as variáveis do arquivo .env
load_dotenv()

# Obtém as credenciais do .env
REGION_NAME = os.getenv("AWS_DEFAULT_REGION")
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
ATHENA_S3_STAGING_DIR = os.getenv("ATHENA_S3_STAGING_DIR")

# Configurações de conexão
def connect_to_athena():
    try:
        # Criar cliente Athena usando boto3
        athena = boto3.client(
            'athena',
            region_name=REGION_NAME,
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY
        )

        print("Conexão com o Athena bem-sucedida!")

        return athena

    except ClientError as e:
        print(f"Erro ao conectar com o Athena: {e}")
        return None

# Testando a conexão
athena_client = connect_to_athena()

# Se a conexão for bem-sucedida, você pode executar consultas
if athena_client:
    # Definir uma consulta para testar a execução no Athena
    query = """
        SELECT * FROM "db_tecbio_market4u"."bio_licenciados" LIMIT 10;
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': 'db_tecbio_market4u'
            },
            ResultConfiguration={
                'OutputLocation': ATHENA_S3_STAGING_DIR
            }
        )

        query_execution_id = response['QueryExecutionId']
        print(f"Consulta iniciada com o ID: {query_execution_id}")

    except ClientError as e:
        print(f"Erro ao executar consulta no Athena: {e}")
else:
    print("Não foi possível estabelecer conexão com o Athena.")
