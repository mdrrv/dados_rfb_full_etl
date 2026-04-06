"""
Script standalone: popula cnpj_consolidado em chunks por faixa de cnpj_basico.

Estratégia: sem carregar empresa/simples na RAM.
- Divide estabelecimento em 10 faixas por primeiro dígito de cnpj_basico (0-9)
- Para cada faixa: PostgreSQL faz o JOIN com índices, COPY TO STDOUT → Python
- Python grava no cnpj_consolidado via COPY FROM STDIN

Uso:
    cd /root/dados_rfb_full_etl
    source /root/etl_venv/bin/activate
    nohup python -u code/consolidar.py > /root/consolidar.log 2>&1 &
"""
import os
import pathlib
import sys
import time
from io import StringIO, BytesIO

import polars as pl
import psycopg2
from dotenv import load_dotenv

# ── env ──────────────────────────────────────────────────────────────────────
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

# ── conexões ─────────────────────────────────────────────────────────────────
conn = psycopg2.connect(DSN)

# ── truncate ─────────────────────────────────────────────────────────────────
print("Truncando cnpj_consolidado...", flush=True)
with conn.cursor() as c:
    c.execute(f'TRUNCATE TABLE "{db_schema}"."cnpj_consolidado";')
conn.commit()

# ── verifica índice em estabelecimento ───────────────────────────────────────
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

# ── JOIN por faixas de cnpj_basico ────────────────────────────────────────────
# cnpj_basico é texto de 8 dígitos — primeiro dígito varia de '0' a '9'
# Isso distribui os ~70M registros em ~10 chunks sem OFFSET

FINAL_COLS = [
    'cnpj', 'cnpj_basico', 'razao_social', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'data_inicio_atividade',
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

SELECT_SQL = f"""
    SELECT
        es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv        AS cnpj,
        es.cnpj_basico,
        emp.razao_social,
        es.nome_fantasia,
        es.situacao_cadastral,
        es.data_situacao_cadastral,
        es.data_inicio_atividade,
        es.cnae_fiscal_principal,
        c.descricao                                           AS desc_cnae_principal,
        emp.natureza_juridica,
        nj.descricao                                          AS desc_natureza_juridica,
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
        mu.descricao                                          AS nome_municipio,
        es.ddd_1,
        es.telefone_1,
        es.correio_eletronico
    FROM "{db_schema}"."estabelecimento" es
    LEFT JOIN "{db_schema}"."empresa" emp ON emp.cnpj_basico = es.cnpj_basico
    LEFT JOIN "{db_schema}"."cnae"    c   ON c.codigo        = es.cnae_fiscal_principal
    LEFT JOIN "{db_schema}"."natju"   nj  ON nj.codigo       = emp.natureza_juridica
    LEFT JOIN "{db_schema}"."munic"   mu  ON mu.codigo       = es.municipio
    LEFT JOIN "{db_schema}"."simples" si  ON si.cnpj_basico  = es.cnpj_basico
    WHERE es.cnpj_basico LIKE %s
    ON CONFLICT DO NOTHING
"""

COPY_SELECT = f"""
    SELECT
        es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv        AS cnpj,
        es.cnpj_basico,
        emp.razao_social,
        es.nome_fantasia,
        es.situacao_cadastral,
        es.data_situacao_cadastral,
        es.data_inicio_atividade,
        es.cnae_fiscal_principal,
        c.descricao                                           AS desc_cnae_principal,
        emp.natureza_juridica,
        nj.descricao                                          AS desc_natureza_juridica,
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
        mu.descricao                                          AS nome_municipio,
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
    INSERT INTO "{db_schema}"."cnpj_consolidado" ({col_list})
    {COPY_SELECT}
    ON CONFLICT (cnpj) DO NOTHING;
"""

print("\nIniciando consolidação por faixa de cnpj_basico...", flush=True)
total_inserted = 0
start = time.time()

for digit in range(10):
    t0 = time.time()
    pattern = f"{digit}%"
    print(f"  Faixa {digit}x... ", end='', flush=True)

    with conn.cursor() as c:
        c.execute(
            INSERT_SQL.replace('{digit}', str(digit))
        )
        n = c.rowcount
        conn.commit()

    total_inserted += n
    print(f"{n:,} inseridos ({round(time.time()-t0)}s) — total: {total_inserted:,}", flush=True)

conn.close()

total_secs = round(time.time() - start)
print(f"\ncnpj_consolidado populado! {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)")
