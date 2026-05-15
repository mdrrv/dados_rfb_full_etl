"""
Script standalone: popula cnpj_consolidado em chunks por faixa de cnpj_basico.

Estratégia zero-downtime: escreve em cnpj_consolidado_new, depois swap atômico.
A tabela de produção fica intacta até o build estar 100% completo.

Uso:
    cd /root/app/dados_rfb_full_etl
    source /root/venv-etl/bin/activate
    nohup python -u code/consolidar.py > /root/consolidar.log 2>&1 &
"""
import os
import pathlib
import sys
import time

import psycopg2
from dotenv import load_dotenv

current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, '.env')
if not os.path.isfile(dotenv_path):
    print(f"Arquivo .env não encontrado em {dotenv_path}")
    sys.exit(1)
load_dotenv(dotenv_path=dotenv_path)

user      = os.getenv('DB_USER')
password  = os.getenv('DB_PASSWORD')
host      = os.getenv('DB_HOST')
port      = os.getenv('DB_PORT')
database  = os.getenv('DB_NAME')
db_schema = os.getenv('DB_SCHEMA')

DSN = f"dbname={database} user={user} host={host} port={port} password={password}"

conn = psycopg2.connect(DSN)

# Cria tabela _new (mesma estrutura, sem índices para inserção rápida)
print("Criando cnpj_consolidado_new...", flush=True)
with conn.cursor() as c:
    c.execute(f'DROP TABLE IF EXISTS "{db_schema}"."cnpj_consolidado_new" CASCADE')
    c.execute(f'''
        CREATE TABLE "{db_schema}"."cnpj_consolidado_new"
        (LIKE "{db_schema}"."cnpj_consolidado" INCLUDING DEFAULTS)
    ''')
conn.commit()

# Verifica índice em estabelecimento
print("Verificando índice em estabelecimento.cnpj_basico...", flush=True)
with conn.cursor() as c:
    c.execute("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname = %s AND tablename = 'estabelecimento'
        AND indexdef LIKE '%%cnpj_basico%%'
        LIMIT 1;
    """, (db_schema,))
    idx = c.fetchone()
    if not idx:
        print("  Criando índice (pode levar alguns minutos)...", flush=True)
        t0 = time.time()
        c.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_cnpj_basico ON "{db_schema}"."estabelecimento" (cnpj_basico);')
        conn.commit()
        print(f"  Índice criado em {round(time.time()-t0)}s", flush=True)
    else:
        print(f"  Índice encontrado: {idx[0]}", flush=True)

print("Ajustando work_mem...", flush=True)
with conn.cursor() as c:
    c.execute("SET work_mem = '256MB';")
conn.commit()

FINAL_COLS = [
    'cnpj', 'cnpj_basico', 'razao_social', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'data_inicio_atividade',
    'cnae_fiscal_principal', 'desc_cnae_principal',
    'natureza_juridica', 'desc_natureza_juridica',
    'capital_social', 'porte_empresa',
    'opcao_pelo_simples', 'data_opcao_simples',
    'opcao_mei', 'data_opcao_mei',
    'identificador_mf',
    'logradouro', 'numero', 'complemento', 'bairro', 'cep',
    'uf', 'municipio', 'nome_municipio',
    'ddd_1', 'telefone_1', 'correio_eletronico',
]

col_list = ', '.join(f'"{c}"' for c in FINAL_COLS)

COPY_SELECT = f"""
    SELECT
        es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv        AS cnpj,
        es.cnpj_basico,
        emp.razao_social,
        es.nome_fantasia,
        es.situacao_cadastral,
        es.data_situacao_cadastral,
        es.motivo_situacao_cadastral,
        es.data_inicio_atividade,
        es.cnae_fiscal_principal,
        c.descricao                                          AS desc_cnae_principal,
        emp.natureza_juridica,
        nj.descricao                                         AS desc_natureza_juridica,
        emp.capital_social,
        emp.porte_empresa,
        si.opcao_pelo_simples,
        si.data_opcao_simples,
        si.opcao_mei,
        si.data_opcao_mei,
        es.identificador_matriz_filial                        AS identificador_mf,
        es.logradouro,
        es.numero,
        es.complemento,
        es.bairro,
        es.cep,
        es.uf,
        es.municipio,
        mu.descricao                                         AS nome_municipio,
        es.ddd_1,
        es.telefone_1,
        es.correio_eletronico
    FROM "{db_schema}"."estabelecimento" es
    LEFT JOIN "{db_schema}"."empresa" emp ON emp.cnpj_basico = es.cnpj_basico
    LEFT JOIN "{db_schema}"."cnae"    c   ON c.codigo        = es.cnae_fiscal_principal
    LEFT JOIN "{db_schema}"."natju"   nj  ON nj.codigo       = emp.natureza_juridica
    LEFT JOIN "{db_schema}"."munic"   mu  ON mu.codigo       = es.municipio
    LEFT JOIN "{db_schema}"."simples" si  ON si.cnpj_basico  = es.cnpj_basico
    WHERE es.cnpj_basico LIKE '{{digit}}%'
"""

INSERT_SQL = f"""
    INSERT INTO "{db_schema}"."cnpj_consolidado_new" ({col_list})
    {COPY_SELECT}
    ON CONFLICT DO NOTHING;
"""

print("\nIniciando consolidação em 100 pedaços (00 a 99)...", flush=True)
total_inserted = 0
start = time.time()

for digit in range(100):
    t0 = time.time()
    digit_str = f"{digit:02d}"
    print(f"  Faixa {digit_str}... ", end='', flush=True)
    with conn.cursor() as c:
        c.execute(INSERT_SQL.replace('{digit}', digit_str))
        n = c.rowcount
        conn.commit()
    total_inserted += n
    print(f"{n:,} inseridos ({round(time.time()-t0)}s) — total: {total_inserted:,}", flush=True)

total_secs = round(time.time() - start)
print(f"\ncnpj_consolidado_new populado: {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)", flush=True)

# Índices em _new antes do swap
print("\nCriando índices em cnpj_consolidado_new...", flush=True)
conn2 = psycopg2.connect(DSN)
conn2.autocommit = True

INDEXES = [
    ("idx_cnpj_cons_new_cnpj_basico", "cnpj_consolidado_new", "cnpj_basico"),
    ("idx_cnpj_cons_new_cep",         "cnpj_consolidado_new", "cep"),
    ("idx_cnpj_cons_new_email",       "cnpj_consolidado_new", "correio_eletronico"),
    ("idx_cnpj_cons_new_cnae",        "cnpj_consolidado_new", "cnae_fiscal_principal"),
    ("idx_cnpj_cons_new_uf",          "cnpj_consolidado_new", "uf"),
]
for idx_name, table, column in INDEXES:
    t0 = time.time()
    print(f"  {idx_name}... ", end='', flush=True)
    with conn2.cursor() as c:
        c.execute(f'CREATE INDEX {idx_name} ON "{db_schema}"."{table}" ({column})')
    print(f"({round(time.time()-t0)}s)", flush=True)

conn2.close()

# ANALYZE antes do swap para estatísticas corretas
print("\nANALYZE em cnpj_consolidado_new...", flush=True)
with conn.cursor() as c:
    c.execute(f'ANALYZE "{db_schema}"."cnpj_consolidado_new"')
conn.commit()

# Swap atômico: _new → produção, produção → _old, drop _old
print("\nSwap atômico cnpj_consolidado_new → cnpj_consolidado...", flush=True)
with conn.cursor() as c:
    c.execute(f'ALTER TABLE "{db_schema}"."cnpj_consolidado"     RENAME TO "cnpj_consolidado_old"')
    c.execute(f'ALTER TABLE "{db_schema}"."cnpj_consolidado_new" RENAME TO "cnpj_consolidado"')
    c.execute(f'DROP TABLE  "{db_schema}"."cnpj_consolidado_old"')
conn.commit()
print("Swap concluído. cnpj_consolidado atualizado com zero downtime.", flush=True)

# Verifica índice trigrama (criado externamente)
with conn.cursor() as c:
    c.execute("SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s",
              (db_schema, 'idx_cnpj_consolidado_razao_trgm'))
    if not c.fetchone():
        print("AVISO: idx_cnpj_consolidado_razao_trgm não existe — crie manualmente com pg_trgm.", flush=True)
    else:
        print("OK: idx_cnpj_consolidado_razao_trgm presente.", flush=True)

conn.close()
print("\nProcesso Finalizado com Sucesso!", flush=True)
