"""
Script standalone: popula cnpj_consolidado via Polars (JOIN em memória).
Usa as tabelas fonte já existentes no banco — não faz download nem re-insert.

Uso:
    cd /root/dados_rfb_full_etl
    source /root/etl_venv/bin/activate
    nohup python -u code/consolidar.py > /root/consolidar.log 2>&1 &
"""
import gc
import os
import pathlib
import sys
import time
from io import StringIO

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

# ── helpers ──────────────────────────────────────────────────────────────────
def to_sql(df: pl.DataFrame, table_name: str, conn, schema: str):
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


def fetch_lookup(conn, name, query, chunk_size=1_000_000):
    """Carrega tabela de lookup no Polars via server-side cursor (baixo pico de RAM)."""
    parts = []
    with conn.cursor(f'lookup_{name}') as lc:
        lc.execute(query)
        cols = [d[0] for d in lc.description]
        while True:
            rows = lc.fetchmany(chunk_size)
            if not rows:
                break
            data = {col: [r[i] for r in rows] for i, col in enumerate(cols)}
            parts.append(pl.DataFrame(data))
            del rows, data
    conn.commit()
    if not parts:
        return pl.DataFrame()
    df = pl.concat(parts)
    del parts
    gc.collect()
    print(f"  {name}: {len(df):,} linhas", flush=True)
    return df


# ── conexões ─────────────────────────────────────────────────────────────────
conn      = psycopg2.connect(DSN)  # leitura (named cursor)
conn_write = psycopg2.connect(DSN)  # escrita (COPY)
cur       = conn.cursor()

# ── truncate ─────────────────────────────────────────────────────────────────
print("Truncando cnpj_consolidado...", flush=True)
cur.execute(f'TRUNCATE TABLE "{db_schema}"."cnpj_consolidado";')
conn.commit()

# ── lookups ──────────────────────────────────────────────────────────────────
print("\nCarregando lookups na memória (Polars)...")
t0 = time.time()

empresa_df = fetch_lookup(conn, 'empresa', f'SELECT cnpj_basico, razao_social, natureza_juridica, capital_social, porte_empresa FROM "{db_schema}"."empresa"')
simples_df = fetch_lookup(conn, 'simples', f'SELECT cnpj_basico, opcao_pelo_simples, data_opcao_simples, opcao_mei, data_opcao_mei FROM "{db_schema}"."simples"')
cnae_df    = fetch_lookup(conn, 'cnae',    f'SELECT codigo, descricao FROM "{db_schema}"."cnae"')
natju_df   = fetch_lookup(conn, 'natju',   f'SELECT codigo, descricao FROM "{db_schema}"."natju"')
munic_df   = fetch_lookup(conn, 'munic',   f'SELECT codigo, descricao FROM "{db_schema}"."munic"')

empresa_df = empresa_df.with_columns([
    pl.col('cnpj_basico').cast(pl.Utf8),
    pl.col('natureza_juridica').cast(pl.Utf8),
])
simples_df = simples_df.with_columns(pl.col('cnpj_basico').cast(pl.Utf8))
cnae_df    = cnae_df.rename({'codigo': 'cnae_fiscal_principal', 'descricao': 'desc_cnae_principal'}).with_columns(pl.col('cnae_fiscal_principal').cast(pl.Utf8))
natju_df   = natju_df.rename({'codigo': 'natureza_juridica', 'descricao': 'desc_natureza_juridica'}).with_columns(pl.col('natureza_juridica').cast(pl.Utf8))
munic_df   = munic_df.rename({'codigo': 'municipio', 'descricao': 'nome_municipio'}).with_columns(pl.col('municipio').cast(pl.Utf8))

print(f"Lookups prontos em {round(time.time()-t0)}s\n", flush=True)

# ── stream + join + copy ──────────────────────────────────────────────────────
ESTAB_COLS = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'data_inicio_atividade',
    'cnae_fiscal_principal', 'identificador_matriz_filial',
    'logradouro', 'numero', 'complemento', 'bairro', 'cep',
    'uf', 'municipio', 'ddd_1', 'telefone_1', 'correio_eletronico',
]
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

CHUNK_SIZE    = 500_000
chunk_num     = 0
total_inserted = 0
start         = time.time()

with conn.cursor('estab_stream') as sc:
    sc.itersize = CHUNK_SIZE
    sc.execute(f'SELECT {", ".join(ESTAB_COLS)} FROM "{db_schema}"."estabelecimento"')

    while True:
        rows = sc.fetchmany(CHUNK_SIZE)
        if not rows:
            break

        chunk_num += 1
        tc = time.time()

        chunk_df = pl.DataFrame(
            {col: [r[i] for r in rows] for i, col in enumerate(ESTAB_COLS)}
        )
        del rows
        gc.collect()

        chunk_df = chunk_df.with_columns([
            pl.col('cnpj_basico').cast(pl.Utf8),
            pl.col('cnpj_ordem').cast(pl.Utf8),
            pl.col('cnpj_dv').cast(pl.Utf8),
            pl.col('cnae_fiscal_principal').cast(pl.Utf8),
            pl.col('municipio').cast(pl.Utf8),
        ]).with_columns(
            (pl.col('cnpj_basico') + pl.col('cnpj_ordem') + pl.col('cnpj_dv')).alias('cnpj')
        ).rename({'identificador_matriz_filial': 'identificador_mf'})

        result = (
            chunk_df
            .join(empresa_df, on='cnpj_basico', how='left')
            .join(simples_df, on='cnpj_basico', how='left')
            .join(cnae_df,    on='cnae_fiscal_principal', how='left')
            .join(natju_df,   on='natureza_juridica', how='left')
            .join(munic_df,   on='municipio', how='left')
            .select(FINAL_COLS)
        )

        to_sql(result, 'cnpj_consolidado', conn_write, db_schema)
        total_inserted += len(result)
        elapsed = round(time.time() - tc)
        print(f"  Chunk {chunk_num}: {total_inserted:,} inseridos ({elapsed}s)", flush=True)
        del chunk_df, result
        gc.collect()

conn.commit()
conn_write.close()
conn.close()

total_secs = round(time.time() - start)
print(f"\ncnpj_consolidado populado! {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)")
