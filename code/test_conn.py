import os
from dotenv import load_dotenv
import psycopg2

# Carrega as variáveis do arquivo .env (certifique-se de que o .env está em UTF-8)
load_dotenv()

user     = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host     = os.getenv("DB_HOST")
port     = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

# Força o client_encoding para LATIN1 via variável de ambiente
os.environ['PGCLIENTENCODING'] = 'UTF8'

conn_str = f"dbname={database} user={user} host={host} port={port} password={password}"
print("String de conexão:")
print(conn_str)

try:
    conn = psycopg2.connect(conn_str)
    print("\nConexão estabelecida com sucesso!")
    conn.close()
except Exception as e:
    print("\nErro ao conectar:")
    print(e)
