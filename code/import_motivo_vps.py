"""
Passo 2/2 — roda na VPS.
Importa motivo_cnpj.csv.gz (gerado pelo export_motivo_local.py) e atualiza
a coluna motivo_situacao_cadastral em cnpj_consolidado.

Uso:
    cd /root/app/dados_rfb_full_etl
    nohup python3 -u code/import_motivo_vps.py /tmp/motivo_cnpj.csv.gz > /root/motivo_import.log 2>&1 &
    tail -f /root/motivo_import.log
"""
import gzip
import os
import pathlib
import sys
import time
from io import StringIO

import psycopg2
from dotenv import load_dotenv


def _find_dotenv() -> str:
    p = pathlib.Path().resolve()
    for _ in range(5):
        candidate = p / ".env"
        if candidate.is_file():
            return str(candidate)
        p = p.parent
    raise SystemExit("ERRO: .env não encontrado. Execute a partir de /root/app/dados_rfb_full_etl")


load_dotenv(_find_dotenv(), override=True)

db_schema = os.getenv("DB_SCHEMA", "dados_rfb")

# Arquivo de entrada — aceita como argumento ou usa padrão
csv_gz = sys.argv[1] if len(sys.argv) > 1 else "/tmp/motivo_cnpj.csv.gz"
if not os.path.isfile(csv_gz):
    raise SystemExit(f"ERRO: arquivo não encontrado: {csv_gz}")

print(f"Arquivo:  {csv_gz} ({os.path.getsize(csv_gz)/1024/1024:.1f} MB)")
print(f"Banco:    {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')} / {os.getenv('DB_NAME')}")

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    password=os.getenv("DB_PASSWORD"),
    connect_timeout=30,
)
cur = conn.cursor()
print("Conectado.\n")

# ── 1. Coluna ─────────────────────────────────────────────────────────────────
cur.execute(f"""
    ALTER TABLE "{db_schema}"."cnpj_consolidado"
    ADD COLUMN IF NOT EXISTS motivo_situacao_cadastral VARCHAR(2);
""")
conn.commit()
print("Coluna motivo_situacao_cadastral: OK")

# ── 2. Temp table ─────────────────────────────────────────────────────────────
cur.execute("DROP TABLE IF EXISTS _tmp_motivo;")
cur.execute("""
    CREATE TEMP TABLE _tmp_motivo (
        cnpj                      TEXT,
        motivo_situacao_cadastral VARCHAR(2)
    );
""")
conn.commit()

# ── 3. COPY em chunks para não estourar memória ───────────────────────────────
print(f"Carregando {csv_gz} na temp table...", flush=True)
CHUNK = 500_000
buf_lines = []
total = 0
t0 = time.time()

with gzip.open(csv_gz, "rt", encoding="utf-8") as f:
    header = f.readline()  # pula o header
    for line in f:
        buf_lines.append(line)
        if len(buf_lines) >= CHUNK:
            cur2 = conn.cursor()
            cur2.copy_expert(
                "COPY _tmp_motivo (cnpj, motivo_situacao_cadastral) FROM STDIN WITH (FORMAT CSV, NULL '')",
                StringIO("".join(buf_lines)),
            )
            conn.commit()
            cur2.close()
            total += len(buf_lines)
            buf_lines = []
            print(f"  {total:,} linhas carregadas ({round(time.time()-t0)}s)", flush=True)

if buf_lines:
    cur2 = conn.cursor()
    cur2.copy_expert(
        "COPY _tmp_motivo (cnpj, motivo_situacao_cadastral) FROM STDIN WITH (FORMAT CSV, NULL '')",
        StringIO("".join(buf_lines)),
    )
    conn.commit()
    cur2.close()
    total += len(buf_lines)

print(f"Temp table: {total:,} registros em {round(time.time()-t0)}s\n")

# ── 4. UPDATE ─────────────────────────────────────────────────────────────────
print("Executando UPDATE em cnpj_consolidado... (pode levar 10-30 min)", flush=True)
t0 = time.time()
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
print(f"UPDATE: {updated:,} registros ({round(time.time()-t0)}s)\n")

cur.execute("DROP TABLE IF EXISTS _tmp_motivo;")
conn.commit()

# ── 5. Verificação ────────────────────────────────────────────────────────────
cur.execute(f"""
    SELECT motivo_situacao_cadastral, COUNT(*) n
    FROM "{db_schema}"."cnpj_consolidado"
    WHERE motivo_situacao_cadastral IS NOT NULL
      AND motivo_situacao_cadastral NOT IN ('0', '00')
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
""")
print("Top motivos (excluindo 00):")
for motivo, n in cur.fetchall():
    print(f"  {motivo}: {n:,}")

cur.close()
conn.close()

# ── 6. Remove o arquivo temporário ───────────────────────────────────────────
os.remove(csv_gz)
print(f"\nArquivo {csv_gz} removido.")
print("Concluído.")
