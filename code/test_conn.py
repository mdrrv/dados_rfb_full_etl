import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Carregar variáveis do .env
load_dotenv()

# Obtenha as credenciais do .env
REGION_NAME = os.getenv('AWS_DEFAULT_REGION')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def test_athena_connection():
    # Tente criar um cliente Athena
    try:
        athena = boto3.client(
            'athena',
            region_name=REGION_NAME,
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY
        )
        print("Conexão com o Athena bem-sucedida!")
        return True
    except ClientError as e:
        print(f"Erro ao conectar com o Athena: {e}")
        return False

# Testa a conexão
if test_athena_connection():
    print("A conexão foi estabelecida com sucesso.")
else:
    print("Falha ao estabelecer a conexão.")
