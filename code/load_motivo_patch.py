"""
Script pontual: adiciona motivo_situacao_cadastral ao cnpj_consolidado.

Lê apenas os arquivos ESTABELE* (já extraídos), extrai somente cnpj + motivo,
carrega em tabela temporária e faz UPDATE no cnpj_consolidado.
Muito mais rápido e barato em disco do que reprocessar o ETL completo.

Pré-requisito: arquivos ESTABE*.* extraídos em EXTRACTED_FILES_PATH (definido no .env)
"""
import os
import pathlib
import time
from io import StringIO

import polars as pl
import psycopg2
from dotenv import load_dotenv

current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, ".env")
if not os.path.isfile(dotenv_path):
    local_env = input("Informe o local do .env: ")
    dotenv_path = os.path.join(local_env, ".env")
load_dotenv(dotenv_path=dotenv_path)

extracted_files = os.getenv("EXTRACTED_FILES_PATH")
db_schema = os.getenv("DB_SCHEMA", "dados_rfb")

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    password=os.getenv("DB_PASSWORD"),
)
cur = conn.cursor()

# ── 1. Adicionar coluna (idempotente) ────────────────────────────────────────
cur.execute(f"""
    ALTER TABLE "{db_schema}"."cnpj_consolidado"
    ADD COLUMN IF NOT EXISTS motivo_situacao_cadastral VARCHAR(2);
""")
conn.commit()
print("Coluna motivo_situacao_cadastral: OK")

# ── 2. Criar temp table (sem PK para tolerar duplicatas nos arquivos brutos) ──
cur.execute("DROP TABLE IF EXISTS _tmp_motivo;")
cur.execute("""
    CREATE TEMP TABLE _tmp_motivo (
        cnpj               TEXT,
        motivo_situacao_cadastral VARCHAR(2)
    );
""")
conn.commit()

# Layout do arquivo de estabelecimento — apenas as 8 primeiras colunas
# pos 0: cnpj_basico | 1: cnpj_ordem | 2: cnpj_dv | 7: motivo_situacao_cadastral
ALL_ESTAB_COLS = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
    'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
    'motivo_situacao_cadastral',
]

arquivos = sorted([f for f in os.listdir(extracted_files) if "ESTABELE" in f.upper()])
if not arquivos:
    print(f"Nenhum arquivo ESTABE* encontrado em: {extracted_files}")
    raise SystemExit(1)

print(f"{len(arquivos)} arquivo(s) de estabelecimento encontrado(s).")
total_tmp = 0
start = time.time()

for arquivo in arquivos:
    path = os.path.join(extracted_files, arquivo)
    t0 = time.time()
    print(f"Lendo {arquivo}... ", end="", flush=True)

    df = pl.read_csv(
        path,
        separator=";",
        has_header=False,
        encoding="latin1",
        infer_schema_length=0,
        n_columns=8,                 # lê só as 8 primeiras colunas — ignora o resto
        new_columns=ALL_ESTAB_COLS,
    )

    df = df.select([
        (pl.col("cnpj_basico") + pl.col("cnpj_ordem") + pl.col("cnpj_dv")).alias("cnpj"),
        pl.col("motivo_situacao_cadastral"),
    ])

    copy_sql = (
        "COPY _tmp_motivo (cnpj, motivo_situacao_cadastral) "
        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    )
    cur2 = conn.cursor()
    cur2.copy_expert(copy_sql, StringIO(df.write_csv(null_value="")))
    conn.commit()
    cur2.close()

    total_tmp += len(df)
    print(f"{len(df):,} linhas ({round(time.time()-t0)}s)", flush=True)

print(f"\nTemp table: {total_tmp:,} registros em {round(time.time()-start)}s")

# ── 3. UPDATE no cnpj_consolidado ────────────────────────────────────────────
print("Executando UPDATE em cnpj_consolidado... ", end="", flush=True)
t0 = time.time()

# DISTINCT ON para resolver eventuais duplicatas nos arquivos brutos
cur.execute(f"""
    UPDATE "{db_schema}"."cnpj_consolidado" cc
    SET    motivo_situacao_cadastral = t.motivo_situacao_cadastral
    FROM  (
        SELECT DISTINCT ON (cnpj) cnpj, motivo_situacao_cadastral
        FROM   _tmp_motivo
        ORDER  BY cnpj
    ) t
    WHERE  cc.cnpj = t.cnpj;
""")
updated = cur.rowcount
conn.commit()
print(f"{updated:,} registros atualizados ({round(time.time()-t0)}s)")

cur.execute("DROP TABLE IF EXISTS _tmp_motivo;")
conn.commit()

# ── 4. Verificação ────────────────────────────────────────────────────────────
cur.execute(f"""
    SELECT motivo_situacao_cadastral, COUNT(*) AS n
    FROM "{db_schema}"."cnpj_consolidado"
    WHERE motivo_situacao_cadastral IS NOT NULL
      AND motivo_situacao_cadastral <> '0'
      AND motivo_situacao_cadastral <> '00'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;
""")
rows = cur.fetchall()
print("\nTop motivos (excluindo 00):")
for motivo, n in rows:
    print(f"  {motivo}: {n:,}")

cur.close()
conn.close()
print("\nConcluído.")
