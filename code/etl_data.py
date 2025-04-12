import datetime
import gc
import pathlib
import os
import re
import sys
import time
import requests
import urllib.request
import wget
import zipfile
import email.utils 

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import bs4 as bs

#############################################
# Funções de apoio
#############################################
def check_diff(url, file_name):
    """
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    """
    if not os.path.isfile(file_name):
        return True  # arquivo ainda não baixado
    response = requests.head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True  # tamanhos diferentes
    return False  # arquivos idênticos

def makedirs(path):
    """
    Cria o diretório se este não existir.
    """
    if not os.path.exists(path):
        os.makedirs(path)

def to_sql(dataframe, **kwargs):
    """
    Insere os registros no banco de dados em blocos.
    """
    size = 4096  # tamanho do chunk (pode ser parametrizado)
    total = len(dataframe)
    name = kwargs.get('name')

    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))

    for i, df_chunk in enumerate(chunker(dataframe)):
        df_chunk.to_sql(**kwargs)
        index = i * size
        percent = (index * 100) / total
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')
    sys.stdout.write('\n')

def get_latest_update_url_by_name(base_url):
    """
    Captura a lista de subdiretórios no formato YYYY-MM/ e retorna
    a URL correspondente à data (ano-mês) mais recente.
    Exemplo de base_url:
        "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    """
    response = requests.get(base_url)
    if not response.ok:
        return None

    soup = bs.BeautifulSoup(response.text, 'lxml')
    
    # Extrair todos os links no padrão "YYYY-MM/"
    year_month_dirs = []
    for link in soup.find_all('a'):
        href = link.get('href', '')
        # Verifica se corresponde a algo como "2025-02/" ou "2023-10/"
        if re.match(r'^\d{4}-\d{2}/$', href):
            # Remove a barra do final (2025-02)
            year_month_dirs.append(href.strip('/'))
    
    if not year_month_dirs:
        return None
    
    # Converter cada link em (ano, mes) para comparação
    parsed = []
    for ym in year_month_dirs:
        ano, mes = ym.split('-')
        parsed.append((int(ano), int(mes)))
    
    # Ordenar e pegar o último, que será o mais recente
    parsed.sort()  # ordena por ano e depois por mês
    latest = parsed[-1]  # tupla (ano, mes) mais recente
    latest_dir = f"{latest[0]:04d}-{latest[1]:02d}/"  # reconstrói no formato YYYY-MM/
    
    return base_url + latest_dir

def bar_progress(current, total, width=80):
    """
    Função de callback para exibir o progresso no download.
    """
    progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

def truncate_tables(cursor, connection, tables, schema):
    """
    Verifica se cada tabela da lista já existe no banco e, se sim, executa o comando
    TRUNCATE para limpar seus dados, reiniciando as chaves primárias.
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

# Diretórios conforme configurado no .env
output_files = os.getenv('OUTPUT_FILES_PATH')
extracted_files = os.getenv('EXTRACTED_FILES_PATH')
makedirs(output_files)
makedirs(extracted_files)
print(f"Diretórios definidos:\n output_files: {output_files}\n extracted_files: {extracted_files}")

# Carregar dados do banco e schema
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')
db_schema = os.getenv('DB_SCHEMA')

# Criação da engine com a configuração de search_path para o schema desejado
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}?options=-csearch_path%3D{db_schema}')
conn = psycopg2.connect(f"dbname={database} user={user} host={host} port={port} password={password}")
cur = conn.cursor()

#############################################
# Truncar tabelas existentes (caso já tenham sido criadas)
#############################################
tables = ["empresa", "estabelecimento", "socios", "simples", "cnae", "moti", "munic", "natju", "pais", "quals"]
truncate_tables(cur, conn, tables, db_schema)

#############################################
# Definindo a URL dos dados com base na última atualização
#############################################
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
dados_rf = get_latest_update_url_by_name(BASE_URL)
if not dados_rf:
    print("Não foi possível encontrar a última atualização dos dados.")
    sys.exit(1)
print("Última atualização encontrada:", dados_rf)

#############################################
# Download dos arquivos .zip disponíveis na página
#############################################
raw_html = urllib.request.urlopen(dados_rf).read()
page_items = bs.BeautifulSoup(raw_html, 'lxml')
html_str = str(page_items)
Files = []
text = '.zip'
for m in re.finditer(text, html_str):
    i_start = m.start() - 40
    i_end = m.end()
    i_loc = html_str[i_start:i_end].find('href=') + 6
    Files.append(html_str[i_start+i_loc:i_end])
# Correção do nome dos arquivos: removendo entradas que contenham '.zip">' na string
Files_clean = [f for f in Files if f.find('.zip">') == -1]
Files = Files_clean

print("Arquivos que serão baixados:")
for idx, f in enumerate(Files, 1):
    print(f"{idx} - {f}")

# Realiza o download dos arquivos
i_l = 0
for file_entry in Files:
    i_l += 1
    url = dados_rf + file_entry
    file_name = os.path.join(output_files, file_entry)
    if check_diff(url, file_name):
        print(f"\nBaixando arquivo {i_l} - {file_entry} ...")
        wget.download(url, out=output_files, bar=bar_progress)
    else:
        print(f"\nO arquivo {i_l} - {file_entry} já existe na pasta destino e está completo. Pulando o download.")

#############################################
# Extração dos arquivos .zip baixados
#############################################
i_l = 0
for file_entry in Files:
    try:
        i_l += 1
        print("Descompactando arquivo:")
        print(f"{i_l} - {file_entry}")
        full_path = os.path.join(output_files, file_entry)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except Exception as e:
        print(f"Erro ao descompactar {file_entry}: {e}")
        pass

#############################################
# Início do processo ETL: Leitura, transformação e inserção dos arquivos no banco
#############################################
# Separa os arquivos extraídos de acordo com cada tipo.
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
    else:
        pass

#############################################
# Processamento dos Arquivos de EMPRESA
#############################################
empresa_insert_start = time.time()
print("\n#######################\n## Arquivos de EMPRESA:\n#######################")
for arquivo in arquivos_empresa:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del empresa
    except:
        pass
    # Definindo os dtypes conforme necessário
    empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    empresa = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=empresa_dtypes,
        encoding='latin-1'
    )
    empresa = empresa.reset_index(drop=True)
    empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 
                       'qualificacao_responsavel', 'capital_social', 'porte_empresa', 
                       'ente_federativo_responsavel']
    # Substituir vírgula por ponto em capital_social e converter para float
    empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',', '.'))
    empresa['capital_social'] = empresa['capital_social'].astype(float)
    # Inserir os dados na tabela "empresa" com o schema definido
    to_sql(empresa, name='empresa', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del empresa
except:
    pass
empresa_insert_end = time.time()
print("Tempo de execução do processo de EMPRESA (segundos):", round((empresa_insert_end - empresa_insert_start)))

#############################################
# Processamento dos Arquivos de ESTABELECIMENTO
#############################################
estabelecimento_insert_start = time.time()
print("\n###############################\n## Arquivos de ESTABELECIMENTO:\n###############################")
print(f"Tem {len(arquivos_estabelecimento)} arquivos de estabelecimento!")
for arquivo in arquivos_estabelecimento:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del estabelecimento
        gc.collect()
    except:
        pass
    estabelecimento_dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                              7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                              14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                              20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                              26: object, 27: object, 28: object, 29: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    NROWS = 2000000
    part = 0
    while True:
        estabelecimento = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            nrows=NROWS,
            skiprows=NROWS * part,
            header=None,
            dtype=estabelecimento_dtypes,
            encoding='latin-1'
        )
        estabelecimento = estabelecimento.reset_index(drop=True)
        estabelecimento.columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                                   'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                                   'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
                                   'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                                   'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                                   'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
                                   'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico',
                                   'situacao_especial', 'data_situacao_especial']
        # Inserir os dados na tabela "estabelecimento" com schema
        to_sql(estabelecimento, name='estabelecimento', con=engine, if_exists='append', index=False, schema=db_schema)
        print(f"Arquivo {arquivo} / parte {part} inserido com sucesso no banco de dados!")
        if len(estabelecimento) == NROWS:
            part += 1
        else:
            break
    try:
        del estabelecimento
    except:
        pass
estabelecimento_insert_end = time.time()
print("Tempo de execução do processo de ESTABELECIMENTO (segundos):", round((estabelecimento_insert_end - estabelecimento_insert_start)))

#############################################
# Processamento dos Arquivos de SÓCIOS
#############################################
socios_insert_start = time.time()
print("\n######################\n## Arquivos de SÓCIOS:\n######################")
for arquivo in arquivos_socios:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del socios
    except:
        pass
    socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                     7: object, 8: object, 9: 'Int32', 10: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    socios = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=socios_dtypes,
        encoding='latin-1'
    )
    socios = socios.reset_index(drop=True)
    socios.columns = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
                      'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
                      'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']
    to_sql(socios, name='socios', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del socios
except:
    pass
socios_insert_end = time.time()
print("Tempo de execução do processo de SÓCIOS (segundos):", round((socios_insert_end - socios_insert_start)))

#############################################
# Processamento dos Arquivos do SIMPLES NACIONAL
#############################################
simples_insert_start = time.time()
print("\n################################\n## Arquivos do SIMPLES NACIONAL:\n################################")
for arquivo in arquivos_simples:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del simples
    except:
        pass
    simples_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    # Verificar número de linhas
    simples_lenght = sum(1 for line in open(extracted_file_path, "r", encoding="latin-1"))
    print(f"Linhas no arquivo do Simples {arquivo}: {simples_lenght}")
    tamanho_das_partes = 1000000  # registros por carga
    partes = round(simples_lenght / tamanho_das_partes)
    nrows = tamanho_das_partes
    skiprows = 0
    print(f"Este arquivo será dividido em {partes} partes para inserção no banco de dados.")
    for i in range(0, partes):
        print(f"Iniciando a parte {i+1} [...]")
        simples = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            nrows=nrows,
            skiprows=skiprows,
            header=None,
            dtype=simples_dtypes,
            encoding='latin-1'
        )
        simples = simples.reset_index(drop=True)
        simples.columns = ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples',
                           'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei',
                           'data_exclusao_mei']
        skiprows += nrows
        to_sql(simples, name='simples', con=engine, if_exists='append', index=False, schema=db_schema)
        print(f"Arquivo {arquivo} parte {i+1} inserido com sucesso no banco de dados!")
        try:
            del simples
        except:
            pass
try:
    del simples
except:
    pass
simples_insert_end = time.time()
print("Tempo de execução do processo do SIMPLES (segundos):", round((simples_insert_end - simples_insert_start)))

#############################################
# Processamento dos Arquivos de CNAE
#############################################
cnae_insert_start = time.time()
print("\n######################\n## Arquivos de CNAE:\n######################")
for arquivo in arquivos_cnae:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del cnae
    except:
        pass
    extracted_file_path = os.path.join(extracted_files, arquivo)
    cnae = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype='object',
        encoding='latin-1'
    )
    cnae = cnae.reset_index(drop=True)
    cnae.columns = ['codigo', 'descricao']
    to_sql(cnae, name='cnae', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del cnae
except:
    pass
cnae_insert_end = time.time()
print("Tempo de execução do processo de CNAE (segundos):", round((cnae_insert_end - cnae_insert_start)))

#############################################
# Processamento dos Arquivos de MOTIVOS (MOTI)
#############################################
moti_insert_start = time.time()
print("\n#########################################\n## Arquivos de MOTIVOS:\n#########################################")
for arquivo in arquivos_moti:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del moti
    except:
        pass
    moti_dtypes = {0: 'Int32', 1: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    moti = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=moti_dtypes,
        encoding='latin-1'
    )
    moti = moti.reset_index(drop=True)
    moti.columns = ['codigo', 'descricao']
    to_sql(moti, name='moti', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del moti
except:
    pass
moti_insert_end = time.time()
print("Tempo de execução do processo de MOTI (segundos):", round((moti_insert_end - moti_insert_start)))

#############################################
# Processamento dos Arquivos de MUNICÍPIOS (MUNIC)
#############################################
munic_insert_start = time.time()
print("\n##########################\n## Arquivos de MUNICÍPIOS:\n##########################")
for arquivo in arquivos_munic:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del munic
    except:
        pass
    munic_dtypes = {0: 'Int32', 1: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    munic = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=munic_dtypes,
        encoding='latin-1'
    )
    munic = munic.reset_index(drop=True)
    munic.columns = ['codigo', 'descricao']
    to_sql(munic, name='munic', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del munic
except:
    pass
munic_insert_end = time.time()
print("Tempo de execução do processo de MUNICÍPIOS (segundos):", round((munic_insert_end - munic_insert_start)))

#############################################
# Processamento dos Arquivos de NATUREZA JURÍDICA (NATJU)
#############################################
natju_insert_start = time.time()
print("\n#################################\n## Arquivos de NATUREZA JURÍDICA:\n#################################")
for arquivo in arquivos_natju:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del natju
    except:
        pass
    natju_dtypes = {0: 'Int32', 1: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    natju = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=natju_dtypes,
        encoding='latin-1'
    )
    natju = natju.reset_index(drop=True)
    natju.columns = ['codigo', 'descricao']
    to_sql(natju, name='natju', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del natju
except:
    pass
natju_insert_end = time.time()
print("Tempo de execução do processo de NATUREZA JURÍDICA (segundos):", round((natju_insert_end - natju_insert_start)))

#############################################
# Processamento dos Arquivos de PAÍS
#############################################
pais_insert_start = time.time()
print("\n######################\n## Arquivos de PAÍS:\n######################")
for arquivo in arquivos_pais:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del pais
    except:
        pass
    pais_dtypes = {0: 'Int32', 1: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    pais = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=pais_dtypes,
        encoding='latin-1'
    )
    pais = pais.reset_index(drop=True)
    pais.columns = ['codigo', 'descricao']
    to_sql(pais, name='pais', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del pais
except:
    pass
pais_insert_end = time.time()
print("Tempo de execução do processo de PAÍS (segundos):", round((pais_insert_end - pais_insert_start)))

#############################################
# Processamento dos Arquivos de QUALIFICAÇÃO DE SÓCIOS (QUALS)
#############################################
quals_insert_start = time.time()
print("\n######################################\n## Arquivos de QUALIFICAÇÃO DE SÓCIOS:\n######################################")
for arquivo in arquivos_quals:
    print(f"Trabalhando no arquivo: {arquivo} [...]")
    try:
        del quals
    except:
        pass
    quals_dtypes = {0: 'Int32', 1: object}
    extracted_file_path = os.path.join(extracted_files, arquivo)
    quals = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        dtype=quals_dtypes,
        encoding='latin-1'
    )
    quals = quals.reset_index(drop=True)
    quals.columns = ['codigo', 'descricao']
    to_sql(quals, name='quals', con=engine, if_exists='append', index=False, schema=db_schema)
    print(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")
try:
    del quals
except:
    pass
quals_insert_end = time.time()
print("Tempo de execução do processo de QUALIFICAÇÃO DE SÓCIOS (segundos):", round((quals_insert_end - quals_insert_start)))

#############################################
# Finalizando a carga e informando o tempo total
#############################################
insert_end = time.time()
Tempo_insert = round((insert_end - empresa_insert_start))  # Exemplo: utilização do tempo do primeiro processo
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
print("Tempo para criar os índices (segundos):", round((index_end - index_start)))

print("\nProcesso 100% finalizado! Você já pode usar seus dados no BD!")
