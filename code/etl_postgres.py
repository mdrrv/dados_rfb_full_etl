import datetime
import gc
import pathlib
import os
import re
import sys
import time
import requests
import zipfile
import xml.etree.ElementTree as ET
import polars as pl
import psycopg2
from io import StringIO
from dotenv import load_dotenv
import shutil
import random

#############################################
# Controle de Execução (Log)
#############################################
etl_start_time = time.time()
etl_status = 'Sucesso'

#############################################
# Funções de apoio
#############################################
def check_diff(url, file_name, auth=None):
    """
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo tamanho.
    """
    if not os.path.isfile(file_name):
        return True

    try:
        response = requests.head(url, auth=auth, timeout=30)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True
        return False
    except:
        return True

def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def to_sql(df: pl.DataFrame, table_name: str, conn, schema: str):
    """
    Insere os registros no banco via PostgreSQL COPY FROM STDIN (bulk insert nativo).
    Muito mais rápido do que INSERT por lotes.
    """
    cur = conn.cursor()
    col_list = ', '.join(f'"{c}"' for c in df.columns)
    copy_sql = (
        f'COPY "{schema}"."{table_name}" ({col_list}) '
        f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    )
    csv_data = df.write_csv(null_value='').replace('\x00', '')
    cur.copy_expert(copy_sql, StringIO(csv_data))
    conn.commit()
    cur.close()

def get_latest_update_url_by_name():
    """
    Lista as pastas via protocolo WebDAV do Nextcloud e retorna a URL da mais recente.
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    url = "https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ/"
    auth = ('gn672Ad4CF8N6TK', '')

    session_init = requests.Session()
    retries = Retry(total=5, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
    session_init.mount('https://', HTTPAdapter(max_retries=retries))

    headers = {
        'Depth': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    print("Conectando ao servidor da Receita Federal para buscar a pasta mais recente...")

    try:
        response = session_init.request('PROPFIND', url, auth=auth, headers=headers, timeout=(30, 60))
        response.raise_for_status()
    except Exception as e:
        print(f"Falha ao conectar no servidor raiz da Receita Federal: {e}")
        return None

    root = ET.fromstring(response.content)
    namespaces = {'d': 'DAV:'}

    year_month_dirs = []
    for node in root.findall('d:response', namespaces):
        href = node.find('d:href', namespaces).text
        match = re.search(r'/(\d{4}-\d{2})/?$', href)
        if match:
            year_month_dirs.append(match.group(1))

    if not year_month_dirs:
        return None

    year_month_dirs.sort()
    latest_dir = year_month_dirs[-1]

    return url + latest_dir + "/"

def truncate_tables(cursor, connection, tables, schema):
    """
    Verifica se cada tabela da lista já existe no banco e, se sim, executa o TRUNCATE.
    """
    for table in tables:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """, (schema, table))
        exists = cursor.fetchone()[0]
        if exists:
            print(f"Truncando tabela '{schema}.{table}'...")
            cursor.execute(f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY;')
            connection.commit()

#############################################
# Carregar variáveis de ambiente a partir do .env
#############################################
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, '.env')
if not os.path.isfile(dotenv_path):
    print('Especifique o local do seu arquivo de configuração ".env". Por exemplo: C:\\...\\seu_projeto')
    local_env = input("Informe o local onde se encontra o .env: ")
    dotenv_path = os.path.join(local_env, '.env')
print("Usando arquivo .env em:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

output_files = os.getenv('OUTPUT_FILES_PATH')
extracted_files = os.getenv('EXTRACTED_FILES_PATH')
makedirs(output_files)
makedirs(extracted_files)
print(f"Diretórios definidos:\n output_files: {output_files}\n extracted_files: {extracted_files}")

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')
db_schema = os.getenv('DB_SCHEMA')

conn = psycopg2.connect(f"dbname={database} user={user} host={host} port={port} password={password}")
cur = conn.cursor()

#############################################
# Truncar tabelas existentes
#############################################
tables = ["empresa", "estabelecimento", "socios", "simples", "cnae", "moti", "munic", "natju", "pais", "quals"]
truncate_tables(cur, conn, tables, db_schema)

#############################################
# Definindo a URL dos dados com base na última atualização
#############################################
dados_rf = get_latest_update_url_by_name()

if not dados_rf:
    print("Não foi possível encontrar a última atualização dos dados.")
    sys.exit(1)
print("Última atualização encontrada:", dados_rf)

#############################################
# Listagem e Download dos arquivos .zip
#############################################
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

auth = ('gn672Ad4CF8N6TK', '')

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

headers = {
    'Depth': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

print("Buscando lista de arquivos na pasta mais recente via WebDAV...")
try:
    response_zips = session.request('PROPFIND', dados_rf, auth=auth, headers=headers, timeout=60)
    response_zips.raise_for_status()
except Exception as e:
    print(f"Erro fatal ao se comunicar com a Receita Federal: {e}")
    etl_status = f'Falha: {str(e)[:100]}'

root_zips = ET.fromstring(response_zips.content)
namespaces = {'d': 'DAV:'}

Files = []
for node in root_zips.findall('d:response', namespaces):
    href = node.find('d:href', namespaces).text
    if href.endswith('.zip'):
        file_name = href.split('/')[-1]
        Files.append(file_name)

print("Arquivos que serão baixados:")
for idx, f in enumerate(Files, 1):
    print(f"{idx} - {f}")

headers_download = {'User-Agent': headers['User-Agent']}
max_tentativas_download = 5

for i_l, file_entry in enumerate(Files, 1):
    url = dados_rf + file_entry
    file_name = os.path.join(output_files, file_entry)

    if check_diff(url, file_name, auth=auth):
        for tentativa in range(1, max_tentativas_download + 1):
            print(f"\nBaixando arquivo {i_l} - {file_entry} (Tentativa {tentativa}/{max_tentativas_download}) ...")
            try:
                with session.get(url, auth=auth, headers=headers_download, stream=True, timeout=(30, 60)) as r:
                    r.raise_for_status()
                    total_length = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    with open(file_name, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_length:
                                    percent = int(downloaded / total_length * 100)
                                    sys.stdout.write(f"\rDownloading: {percent}% [{downloaded} / {total_length}] bytes")
                                    sys.stdout.flush()
                                    if downloaded >= total_length:
                                        break
                    print("\n")
                print("\nDownload concluído com sucesso!")
                break
            except Exception as e:
                print(f"\nA conexão caiu ou travou durante o download: {e}")
                if tentativa == max_tentativas_download:
                    print(f"Limite de tentativas atingido. Pulando o arquivo {file_entry}.")
                    etl_status = f'Falha no arquivo {file_entry}'
                else:
                    tempo_espera = random.randint(5, 120)
                    print(f"Aguardando {tempo_espera} segundos antes de recomeçar o download desse arquivo...")
                    time.sleep(tempo_espera)
    else:
        print(f"\nO arquivo {i_l} - {file_entry} já existe e está completo. Pulando o download.")

#############################################
# Extração dos arquivos .zip baixados
#############################################
for i_l, file_entry in enumerate(Files, 1):
    try:
        print(f"Descompactando arquivo {i_l} - {file_entry}")
        full_path = os.path.join(output_files, file_entry)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except Exception as e:
        print(f"Erro ao descompactar {file_entry}: {e}")

#############################################
# Classificação dos arquivos extraídos
#############################################
Items = [name for name in os.listdir(extracted_files) if os.path.isfile(os.path.join(extracted_files, name))]
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []

for item in Items:
    if "EMPRE" in item:
        arquivos_empresa.append(item)
    elif "ESTABELE" in item:
        arquivos_estabelecimento.append(item)
    elif "SOCIO" in item:
        arquivos_socios.append(item)
    elif "SIMPLES" in item:
        arquivos_simples.append(item)
    elif "CNAE" in item:
        arquivos_cnae.append(item)
    elif "MOTI" in item:
        arquivos_moti.append(item)
    elif "MUNIC" in item:
        arquivos_munic.append(item)
    elif "NATJU" in item:
        arquivos_natju.append(item)
    elif "PAIS" in item:
        arquivos_pais.append(item)
    elif "QUALS" in item:
        arquivos_quals.append(item)

#############################################
# Processamento dos Arquivos de EMPRESA
#############################################
empresa_insert_start = time.time()
print("\n#######################\n## Arquivos de EMPRESA:\n#######################")
EMPRESA_COLS = ['cnpj_basico', 'razao_social', 'natureza_juridica',
                'qualificacao_responsavel', 'capital_social', 'porte_empresa',
                'ente_federativo_responsavel']

for arquivo in arquivos_empresa:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    empresa = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=EMPRESA_COLS,
    )
    empresa = empresa.with_columns([
        pl.col('natureza_juridica').cast(pl.Int32, strict=False),
        pl.col('qualificacao_responsavel').cast(pl.Int32, strict=False),
        pl.col('porte_empresa').cast(pl.Int32, strict=False),
        pl.col('capital_social').str.replace(',', '.', literal=True).cast(pl.Float64, strict=False),
    ])
    to_sql(empresa, 'empresa', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del empresa

empresa_insert_end = time.time()
print("Tempo de execução do processo de EMPRESA (segundos):", round(empresa_insert_end - empresa_insert_start))

#############################################
# Processamento dos Arquivos de ESTABELECIMENTO
#############################################
estabelecimento_insert_start = time.time()
print("\n###############################\n## Arquivos de ESTABELECIMENTO:\n###############################")
print(f"Tem {len(arquivos_estabelecimento)} arquivos de estabelecimento!")

ESTAB_COLS = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
              'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
              'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
              'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
              'tipo_logradouro', 'logradouro', 'numero', 'complemento',
              'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
              'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico',
              'situacao_especial', 'data_situacao_especial']

ESTAB_INT_COLS = ['identificador_matriz_filial', 'situacao_cadastral', 'data_situacao_cadastral',
                  'motivo_situacao_cadastral', 'data_inicio_atividade', 'cnae_fiscal_principal',
                  'municipio', 'data_situacao_especial']

NROWS = 2_000_000

for arquivo in arquivos_estabelecimento:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    skip = 0
    part = 0
    while True:
        chunk = pl.read_csv(
            extracted_file_path,
            separator=';',
            has_header=False,
            encoding='latin1',
            infer_schema_length=0,
            new_columns=ESTAB_COLS,
            n_rows=NROWS,
            skip_rows=skip,
        )
        if chunk.is_empty():
            break
        chunk = chunk.with_columns([
            pl.col(c).cast(pl.Int32, strict=False) for c in ESTAB_INT_COLS
        ])
        to_sql(chunk, 'estabelecimento', conn, db_schema)
        print(f"Arquivo {arquivo} / parte {part} inserida com sucesso!")
        skip += NROWS
        part += 1
        if len(chunk) < NROWS:
            del chunk
            break
        del chunk
        gc.collect()

estabelecimento_insert_end = time.time()
print("Tempo de execução do processo de ESTABELECIMENTO (segundos):", round(estabelecimento_insert_end - estabelecimento_insert_start))

#############################################
# Processamento dos Arquivos de SÓCIOS
#############################################
socios_insert_start = time.time()
print("\n######################\n## Arquivos de SÓCIOS:\n######################")

SOCIOS_COLS = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
               'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
               'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']

SOCIOS_INT_COLS = ['identificador_socio', 'qualificacao_socio', 'qualificacao_representante_legal', 'faixa_etaria']

for arquivo in arquivos_socios:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    socios = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=SOCIOS_COLS,
    )
    socios = socios.with_columns([
        pl.col(c).cast(pl.Int32, strict=False) for c in SOCIOS_INT_COLS
    ])
    to_sql(socios, 'socios', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del socios

socios_insert_end = time.time()
print("Tempo de execução do processo de SÓCIOS (segundos):", round(socios_insert_end - socios_insert_start))

#############################################
# Processamento dos Arquivos do SIMPLES NACIONAL
#############################################
simples_insert_start = time.time()
print("\n################################\n## Arquivos do SIMPLES NACIONAL:\n################################")

SIMPLES_COLS = ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples',
                'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']

SIMPLES_INT_COLS = ['data_opcao_simples', 'data_exclusao_simples', 'data_opcao_mei', 'data_exclusao_mei']

NROWS_SIMPLES = 1_000_000

for arquivo in arquivos_simples:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    skip = 0
    part = 0
    while True:
        chunk = pl.read_csv(
            extracted_file_path,
            separator=';',
            has_header=False,
            encoding='latin1',
            infer_schema_length=0,
            new_columns=SIMPLES_COLS,
            n_rows=NROWS_SIMPLES,
            skip_rows=skip,
        )
        if chunk.is_empty():
            break
        chunk = chunk.with_columns([
            pl.col(c).cast(pl.Int32, strict=False) for c in SIMPLES_INT_COLS
        ])
        to_sql(chunk, 'simples', conn, db_schema)
        print(f"Arquivo {arquivo} parte {part + 1} inserida com sucesso!")
        skip += NROWS_SIMPLES
        part += 1
        if len(chunk) < NROWS_SIMPLES:
            del chunk
            break
        del chunk

simples_insert_end = time.time()
print("Tempo de execução do processo do SIMPLES (segundos):", round(simples_insert_end - simples_insert_start))

#############################################
# Processamento dos Arquivos de CNAE
#############################################
cnae_insert_start = time.time()
print("\n######################\n## Arquivos de CNAE:\n######################")
for arquivo in arquivos_cnae:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    cnae = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    to_sql(cnae, 'cnae', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del cnae

cnae_insert_end = time.time()
print("Tempo de execução do processo de CNAE (segundos):", round(cnae_insert_end - cnae_insert_start))

#############################################
# Processamento dos Arquivos de MOTIVOS (MOTI)
#############################################
moti_insert_start = time.time()
print("\n#########################################\n## Arquivos de MOTIVOS:\n#########################################")
for arquivo in arquivos_moti:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    moti = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    moti = moti.with_columns(pl.col('codigo').cast(pl.Int32, strict=False))
    to_sql(moti, 'moti', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del moti

moti_insert_end = time.time()
print("Tempo de execução do processo de MOTI (segundos):", round(moti_insert_end - moti_insert_start))

#############################################
# Processamento dos Arquivos de MUNICÍPIOS (MUNIC)
#############################################
munic_insert_start = time.time()
print("\n##########################\n## Arquivos de MUNICÍPIOS:\n##########################")
for arquivo in arquivos_munic:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    munic = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    munic = munic.with_columns(pl.col('codigo').cast(pl.Int32, strict=False))
    to_sql(munic, 'munic', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del munic

munic_insert_end = time.time()
print("Tempo de execução do processo de MUNICÍPIOS (segundos):", round(munic_insert_end - munic_insert_start))

#############################################
# Processamento dos Arquivos de NATUREZA JURÍDICA (NATJU)
#############################################
natju_insert_start = time.time()
print("\n#################################\n## Arquivos de NATUREZA JURÍDICA:\n#################################")
for arquivo in arquivos_natju:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    natju = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    natju = natju.with_columns(pl.col('codigo').cast(pl.Int32, strict=False))
    to_sql(natju, 'natju', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del natju

natju_insert_end = time.time()
print("Tempo de execução do processo de NATUREZA JURÍDICA (segundos):", round(natju_insert_end - natju_insert_start))

#############################################
# Processamento dos Arquivos de PAÍS
#############################################
pais_insert_start = time.time()
print("\n######################\n## Arquivos de PAÍS:\n######################")
for arquivo in arquivos_pais:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    pais = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    pais = pais.with_columns(pl.col('codigo').cast(pl.Int32, strict=False))
    to_sql(pais, 'pais', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del pais

pais_insert_end = time.time()
print("Tempo de execução do processo de PAÍS (segundos):", round(pais_insert_end - pais_insert_start))

#############################################
# Processamento dos Arquivos de QUALIFICAÇÃO DE SÓCIOS (QUALS)
#############################################
quals_insert_start = time.time()
print("\n######################################\n## Arquivos de QUALIFICAÇÃO DE SÓCIOS:\n######################################")
for arquivo in arquivos_quals:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    extracted_file_path = os.path.join(extracted_files, arquivo)
    quals = pl.read_csv(
        extracted_file_path,
        separator=';',
        has_header=False,
        encoding='latin1',
        infer_schema_length=0,
        new_columns=['codigo', 'descricao'],
    )
    quals = quals.with_columns(pl.col('codigo').cast(pl.Int32, strict=False))
    to_sql(quals, 'quals', conn, db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
    del quals

quals_insert_end = time.time()
print("Tempo de execução do processo de QUALIFICAÇÃO DE SÓCIOS (segundos):", round(quals_insert_end - quals_insert_start))

#############################################
# Finalizando a carga e informando o tempo total
#############################################
insert_end = time.time()
Tempo_insert = round(insert_end - empresa_insert_start)
print("\n#############################################")
print("## Processo de carga dos arquivos finalizado!")
print(f"Tempo total de execução do processo (segundos): {Tempo_insert}")

#############################################
# Criação de índices nas tabelas
#############################################
index_start = time.time()
cur.execute(f"""
    CREATE INDEX IF NOT EXISTS empresa_cnpj ON "{db_schema}"."empresa"(cnpj_basico);
    CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON "{db_schema}"."estabelecimento"(cnpj_basico);
    CREATE INDEX IF NOT EXISTS socios_cnpj ON "{db_schema}"."socios"(cnpj_basico);
    CREATE INDEX IF NOT EXISTS simples_cnpj ON "{db_schema}"."simples"(cnpj_basico);
""")
conn.commit()
index_end = time.time()
print("Índices criados nas tabelas (empresa, estabelecimento, socios, simples).")
print("Tempo para criar os índices (segundos):", round(index_end - index_start))

#############################################
# Limpeza dos arquivos temporários
#############################################
print("\n#############################################")
print("## Iniciando a limpeza dos arquivos temporários...")
limpeza_start = time.time()

def limpar_diretorio(caminho_pasta):
    for nome_arquivo in os.listdir(caminho_pasta):
        caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
        try:
            if os.path.isfile(caminho_completo) or os.path.islink(caminho_completo):
                os.unlink(caminho_completo)
            elif os.path.isdir(caminho_completo):
                shutil.rmtree(caminho_completo)
        except Exception as e:
            print(f"Falha ao deletar {caminho_completo}. Motivo: {e}")

print(f"Limpando arquivos compactados (.zip) em: {output_files}")
limpar_diretorio(output_files)

print(f"Limpando arquivos extraídos em: {extracted_files}")
limpar_diretorio(extracted_files)

limpeza_end = time.time()
print(f"Arquivos limpos com sucesso! Tempo de limpeza (segundos): {round(limpeza_end - limpeza_start)}")

#############################################
# Gravação do Log de Execução
#############################################
print("\n#############################################")
print("## Gravando log de execução no banco de dados...")

try:
    folder_date = dados_rf.strip('/').split('/')[-1] if dados_rf else 'Desconhecido'
except:
    folder_date = 'Desconhecido'

etl_end_time = time.time()
duracao_total = round(etl_end_time - etl_start_time)

cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{db_schema}"."execution" (
        id SERIAL PRIMARY KEY,
        folder_date VARCHAR(50),
        execution_timestamp TIMESTAMP,
        duration_seconds INTEGER,
        status VARCHAR(50)
    );
""")

cur.execute(f"""
    INSERT INTO "{db_schema}"."execution"
    (folder_date, execution_timestamp, duration_seconds, status)
    VALUES (%s, %s, %s, %s);
""", (folder_date, datetime.datetime.now(), duracao_total, etl_status))

conn.commit()
cur.close()
conn.close()

print(f"Log registrado! Status: {etl_status} | Duração: {duracao_total}s | Referência: {folder_date}")
print("\nProcesso 100% finalizado! Você já pode usar seus dados no BD!")
