"""
Script standalone: popula cnpj_consolidado via Polars (JOIN em memória).
Usa as tabelas fonte já existentes no banco — não faz download nem re-insert.

Estratégia:
  1. Cria TEMP TABLEs deduplicadas no PostgreSQL (empresa_dedup, simples_dedup)
     via DISTINCT ON — sem escrever CSV grande no disco
  2. Carrega as temp tables (~22M linhas cada) no Polars via fetchmany
  3. Stream de estabelecimento via named cursor, JOIN no Polars por chunk
  4. COPY FROM para inserir resultado em cnpj_consolidado

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

TMP_DIR = '/tmp/consolidar_lookups'
os.makedirs(TMP_DIR, exist_ok=True)

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


def load_table_polars(conn, query, chunk_size=2_000_000):
    """Carrega resultado de query no Polars via fetchmany (client-side cursor)."""
    parts = []
    with conn.cursor() as c:
        c.execute(query)
        cols = [d[0] for d in c.description]
        while True:
            rows = c.fetchmany(chunk_size)
            if not rows:
                break
            parts.append(pl.DataFrame(
                {col: [str(r[i]) if r[i] is not None else None for r in rows] for i, col in enumerate(cols)},
                schema={col: pl.Utf8 for col in cols}
            ))
            del rows
            gc.collect()
    return pl.concat(parts) if parts else pl.DataFrame()


def export_small_csv(conn, name, query, csv_path):
    """Exporta tabela pequena via COPY TO CSV."""
    if os.path.isfile(csv_path):
        print(f"  {name}: CSV já existe, pulando.", flush=True)
        return
    with conn.cursor() as c:
        copy_sql = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, NULL '')"
        with open(csv_path, 'w', encoding='utf-8') as f:
            c.copy_expert(copy_sql, f)
    print(f"  {name}: exportado → {csv_path}", flush=True)


# ── conexões ─────────────────────────────────────────────────────────────────
conn       = psycopg2.connect(DSN)  # leitura (stream)
conn_write = psycopg2.connect(DSN)  # escrita (COPY)

# ── truncate ─────────────────────────────────────────────────────────────────
print("Truncando cnpj_consolidado...", flush=True)
with conn.cursor() as c:
    c.execute(f'TRUNCATE TABLE "{db_schema}"."cnpj_consolidado";')
conn.commit()

# ── cria TEMP TABLEs deduplicadas no PostgreSQL ───────────────────────────────
# Muito mais eficiente que exportar CSV de 67M linhas para disco.
# PostgreSQL usa seu próprio temp tablespace (em /var/lib/postgresql).
print("\nCriando tabelas temporárias deduplicadas no PostgreSQL...", flush=True)
t0 = time.time()

with conn.cursor() as c:
    print("  Criando empresa_dedup (DISTINCT ON cnpj_basico)...", flush=True)
    c.execute(f"""
        CREATE TEMP TABLE empresa_dedup AS
        SELECT DISTINCT ON (cnpj_basico)
            cnpj_basico, razao_social, natureza_juridica, capital_social, porte_empresa
        FROM "{db_schema}"."empresa"
        ORDER BY cnpj_basico;
    """)
    conn.commit()
    c.execute("SELECT COUNT(*) FROM empresa_dedup")
    n_emp = c.fetchone()[0]
    print(f"  empresa_dedup: {n_emp:,} linhas ({round(time.time()-t0)}s)", flush=True)

    t1 = time.time()
    print("  Criando simples_dedup (DISTINCT ON cnpj_basico)...", flush=True)
    c.execute(f"""
        CREATE TEMP TABLE simples_dedup AS
        SELECT DISTINCT ON (cnpj_basico)
            cnpj_basico, opcao_pelo_simples, data_opcao_simples, opcao_mei, data_opcao_mei
        FROM "{db_schema}"."simples"
        ORDER BY cnpj_basico;
    """)
    conn.commit()
    c.execute("SELECT COUNT(*) FROM simples_dedup")
    n_sim = c.fetchone()[0]
    print(f"  simples_dedup: {n_sim:,} linhas ({round(time.time()-t1)}s)", flush=True)

print(f"Temp tables prontas em {round(time.time()-t0)}s\n", flush=True)

# ── exporta lookups pequenos para CSV ────────────────────────────────────────
export_small_csv(conn, 'cnae',  f'SELECT codigo, descricao FROM "{db_schema}"."cnae"',  f'{TMP_DIR}/cnae.csv')
export_small_csv(conn, 'natju', f'SELECT codigo, descricao FROM "{db_schema}"."natju"', f'{TMP_DIR}/natju.csv')
export_small_csv(conn, 'munic', f'SELECT codigo, descricao FROM "{db_schema}"."munic"', f'{TMP_DIR}/munic.csv')

# ── carrega lookups no Polars ─────────────────────────────────────────────────
print("\nCarregando lookups dedup no Polars...", flush=True)
t0 = time.time()

empresa_df = load_table_polars(conn, "SELECT * FROM empresa_dedup")
gc.collect()
print(f"  empresa: {len(empresa_df):,}", flush=True)

simples_df = load_table_polars(conn, "SELECT * FROM simples_dedup")
gc.collect()
print(f"  simples: {len(simples_df):,}", flush=True)

cnae_df  = pl.read_csv(f'{TMP_DIR}/cnae.csv',  infer_schema_length=0).rename({'codigo': 'cnae_fiscal_principal', 'descricao': 'desc_cnae_principal'})
natju_df = pl.read_csv(f'{TMP_DIR}/natju.csv',  infer_schema_length=0).rename({'codigo': 'natureza_juridica',     'descricao': 'desc_natureza_juridica'})
munic_df = pl.read_csv(f'{TMP_DIR}/munic.csv',  infer_schema_length=0).rename({'codigo': 'municipio',             'descricao': 'nome_municipio'})

print(f"  cnae: {len(cnae_df):,} | natju: {len(natju_df):,} | munic: {len(munic_df):,}", flush=True)
print(f"Lookups prontos em {round(time.time()-t0)}s\n", flush=True)

# ── stream estabelecimento + join + copy ──────────────────────────────────────
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

CHUNK_SIZE     = 500_000
chunk_num      = 0
total_inserted = 0
start          = time.time()

print("Iniciando stream de estabelecimento...", flush=True)

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
            {col: [r[i] for r in rows] for i, col in enumerate(ESTAB_COLS)},
            schema={col: pl.Utf8 for col in ESTAB_COLS},
        )
        del rows
        gc.collect()

        chunk_df = chunk_df.with_columns(
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

# Limpa CSVs temporários pequenos
for f in ['cnae.csv', 'natju.csv', 'munic.csv']:
    try:
        os.remove(f'{TMP_DIR}/{f}')
    except Exception:
        pass

total_secs = round(time.time() - start)
print(f"\ncnpj_consolidado populado! {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)")
