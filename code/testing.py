import os
import sys
import time
import requests
import pathlib
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from dotenv import load_dotenv

# URL base da Receita Federal para dados do CNPJ
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# Função para garantir que diretórios existam
def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Função para verificar diferença no tamanho dos arquivos e decidir se deve baixar
def check_diff(url, file_name):
    if not os.path.isfile(file_name):
        return True  # Arquivo ainda não foi baixado

    response = requests.head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)
    
    if new_size != old_size:
        os.remove(file_name)
        return True  # Tamanhos diferentes, precisa baixar de novo

    return False  # Arquivos são iguais, não precisa baixar

# Carregar variáveis de ambiente
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, '.env')
if not os.path.isfile(dotenv_path):
    print('Arquivo ".env" não encontrado! Especifique a localização correta.')
    local_env = input()
    dotenv_path = os.path.join(local_env, '.env')

load_dotenv(dotenv_path=dotenv_path)

# Definir diretórios de saída
output_files = os.getenv('OUTPUT_FILES_PATH')
extracted_files = os.getenv('EXTRACTED_FILES_PATH')

makedirs(output_files)
makedirs(extracted_files)

# Obter lista de subdiretórios (anos/meses)
response = requests.get(BASE_URL)
soup = BeautifulSoup(response.text, "html.parser")

subdirs = []
for link in soup.find_all('a', href=True):
    href = link['href']
    if href.endswith('/') and href.startswith('20'):  # Apenas diretórios de ano-mês
        subdirs.append(href)

# Obter arquivos .zip dentro de cada subdiretório
zip_links = []
for subdir in subdirs:
    subdir_url = BASE_URL + subdir  # URL completa do subdiretório
    res_sub = requests.get(subdir_url)
    subsoup = BeautifulSoup(res_sub.text, "html.parser")

    for link in subsoup.find_all('a', href=True):
        file_href = link['href']
        if file_href.endswith('.zip'):
            full_url = subdir_url + file_href
            zip_links.append(full_url)

# Exibir os arquivos que serão baixados
print('Arquivos que serão baixados:')
for i, file_url in enumerate(zip_links, start=1):
    print(f"{i} - {file_url}")

# Baixar arquivos
for file_url in zip_links:
    file_name = os.path.join(output_files, file_url.split("/")[-1])

    if check_diff(file_url, file_name):
        print(f"Baixando {file_url}...")
        with requests.get(file_url, stream=True) as r:
            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download concluído: {file_name}")

# Descompactar arquivos
for file_name in os.listdir(output_files):
    if file_name.endswith(".zip"):
        file_path = os.path.join(output_files, file_name)
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_files)
            print(f"Descompactado: {file_name}")
        except Exception as e:
            print(f"Erro ao descompactar {file_name}: {e}")

# Conectar ao banco de dados
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Processar arquivos descompactados
for extracted_file in os.listdir(extracted_files):
    file_path = os.path.join(extracted_files, extracted_file)
    
    # Determinar tipo de arquivo
    if "EMPRE" in extracted_file:
        table_name = "empresa"
        dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
    elif "ESTABELE" in extracted_file:
        table_name = "estabelecimento"
        dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object}
    elif "SOCIO" in extracted_file:
        table_name = "socios"
        dtypes = {0: object, 1: 'Int32', 2: object, 3: object}
    else:
        print(f"Ignorando {extracted_file}, tipo desconhecido.")
        continue
    
    # Carregar CSV e inserir no banco de dados
    df = pd.read_csv(file_path, sep=';', header=None, dtype=dtypes, encoding='latin-1')
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"Dados de {extracted_file} inseridos na tabela {table_name}.")

print("Processo finalizado!")
