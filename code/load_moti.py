"""
Script pontual: carrega apenas a tabela `moti` (motivos de situação cadastral).
Use quando os arquivos já estão extraídos e você só quer atualizar essa tabela.

Pré-requisito: arquivos MOTI*.* extraídos em EXTRACTED_FILES_PATH (definido no .env)
"""

import os
import pathlib
from io import StringIO

import polars as pl
import psycopg2
from dotenv import load_dotenv

def _find_dotenv() -> str:
    candidate = pathlib.Path().resolve() / ".env"
    if candidate.is_file():
        return str(candidate)
    raw = input("Informe o caminho do .env (arquivo ou pasta): ").strip().strip("'\"")
    p = pathlib.Path(raw)
    return str(p if p.suffix == ".env" or p.name == ".env" else p / ".env")

dotenv_path = _find_dotenv()
load_dotenv(dotenv_path=dotenv_path)
print(f"Carregando config de: {dotenv_path}")

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

# DDL
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{db_schema}"."moti" (
        codigo   INTEGER,
        descricao TEXT
    );
""")
cur.execute(f'TRUNCATE TABLE "{db_schema}"."moti";')
conn.commit()
print("Tabela moti pronta.")

# Localizar arquivos MOTI
arquivos = [f for f in os.listdir(extracted_files) if "MOTI" in f.upper()]
if not arquivos:
    print(f"Nenhum arquivo MOTI encontrado em: {extracted_files}")
    raise SystemExit(1)

print(f"Arquivos encontrados: {arquivos}")

for arquivo in arquivos:
    path = os.path.join(extracted_files, arquivo)
    print(f"Carregando {arquivo}...")
    df = pl.read_csv(
        path,
        separator=";",
        has_header=False,
        encoding="latin1",
        infer_schema_length=0,
        new_columns=["codigo", "descricao"],
    )
    df = df.with_columns(pl.col("codigo").cast(pl.Int32, strict=False))

    col_list = ', '.join(f'"{c}"' for c in df.columns)
    copy_sql = (
        f'COPY "{db_schema}"."moti" ({col_list}) '
        f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    )
    cur2 = conn.cursor()
    cur2.copy_expert(copy_sql, StringIO(df.write_csv(null_value="")))
    conn.commit()
    cur2.close()
    print(f"  {len(df)} registros inseridos.")

cur.execute(f'SELECT COUNT(*) FROM "{db_schema}"."moti"')
print(f"Total na tabela: {cur.fetchone()[0]} registros")

cur.close()
conn.close()
print("Concluído.")
