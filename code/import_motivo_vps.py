"""
Passo 2/2 — roda na VPS.
Importa motivo_cnpj.csv.gz (gerado pelo export_motivo_local.py) e atualiza
a coluna motivo_situacao_cadastral em cnpj_consolidado.

Estratégia:
  - Cria tabela permanente com índice (não temp) para o join ser O(n) com index lookup
  - UPDATE em 100 batches por prefixo de cnpj_basico — cada batch commita sozinho,
    banco fica responsivo, pode ser interrompido e retomado sem perda

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

csv_gz = sys.argv[1] if len(sys.argv) > 1 else "/tmp/motivo_cnpj.csv.gz"
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

# ── 1. Coluna (idempotente) ───────────────────────────────────────────────────
cur.execute(f"""
    ALTER TABLE "{db_schema}"."cnpj_consolidado"
    ADD COLUMN IF NOT EXISTS motivo_situacao_cadastral VARCHAR(2);
""")
conn.commit()
print("Coluna motivo_situacao_cadastral: OK")

# ── 2. Tabela de mapeamento permanente com índice ─────────────────────────────
# Se já existe com dados (de uma execução anterior interrompida), reutiliza.
cur.execute(f"""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema = '{db_schema}' AND table_name = '_map_motivo';
""")
table_exists = cur.fetchone()[0] > 0

if table_exists:
    cur.execute(f'SELECT COUNT(*) FROM "{db_schema}"."_map_motivo" LIMIT 1;')
    existing_rows = cur.fetchone()[0]
else:
    existing_rows = 0

if existing_rows > 0:
    print(f"Tabela _map_motivo já existe com dados — reutilizando (executando apenas UPDATE).")
    total = existing_rows
else:
    if not os.path.isfile(csv_gz):
        raise SystemExit(f"ERRO: arquivo não encontrado: {csv_gz}")
    print(f"Arquivo:  {csv_gz} ({os.path.getsize(csv_gz)/1024/1024:.1f} MB)")

    if table_exists:
        cur.execute(f'DROP TABLE "{db_schema}"."_map_motivo";')
    cur.execute(f"""
        CREATE TABLE "{db_schema}"."_map_motivo" (
            cnpj                      TEXT PRIMARY KEY,
            motivo_situacao_cadastral VARCHAR(2)
        );
    """)
    conn.commit()
    print("Tabela _map_motivo criada.")

    # ── 3. COPY em chunks ─────────────────────────────────────────────────────
    print(f"Carregando {csv_gz}...", flush=True)
    CHUNK = 500_000
    buf_lines = []
    total = 0
    t0 = time.time()

    with gzip.open(csv_gz, "rt", encoding="utf-8") as f:
        f.readline()  # header
        for line in f:
            buf_lines.append(line)
            if len(buf_lines) >= CHUNK:
                cur2 = conn.cursor()
                cur2.copy_expert(
                    f'COPY "{db_schema}"."_map_motivo" (cnpj, motivo_situacao_cadastral) '
                    "FROM STDIN WITH (FORMAT CSV, NULL '')",
                    StringIO("".join(buf_lines)),
                )
                conn.commit()
                cur2.close()
                total += len(buf_lines)
                buf_lines = []
                print(f"  {total:,} linhas ({round(time.time()-t0)}s)", flush=True)

    if buf_lines:
        cur2 = conn.cursor()
        cur2.copy_expert(
            f'COPY "{db_schema}"."_map_motivo" (cnpj, motivo_situacao_cadastral) '
            "FROM STDIN WITH (FORMAT CSV, NULL '')",
            StringIO("".join(buf_lines)),
        )
        conn.commit()
        cur2.close()
        total += len(buf_lines)

    print(f"_map_motivo: {total:,} registros em {round(time.time()-t0)}s")

# ── 4. UPDATE em 100 batches ──────────────────────────────────────────────────
# Filtra _map_motivo por prefixo de cnpj (usa PK B-tree → index range scan rápido)
# Evita seq scan de 70M linhas em cada batch.
print("\nIniciando UPDATE em 100 batches...", flush=True)
total_updated = 0
start = time.time()

for i in range(100):
    prefix_lo = f"{i:02d}"
    prefix_hi = f"{i+1:02d}" if i < 99 else chr(ord('9') + 1)  # batch 99: hi = ':' > all digits
    t0 = time.time()
    cur.execute(f"""
        UPDATE "{db_schema}"."cnpj_consolidado" cc
        SET    motivo_situacao_cadastral = m.motivo_situacao_cadastral
        FROM   "{db_schema}"."_map_motivo" m
        WHERE  cc.cnpj = m.cnpj
          AND  m.cnpj >= %s AND m.cnpj < %s
          AND  cc.motivo_situacao_cadastral IS NULL;
    """, (prefix_lo, prefix_hi))
    n = cur.rowcount
    conn.commit()
    total_updated += n
    print(f"  Batch {prefix_lo}: {n:,} atualizados ({round(time.time()-t0)}s) — total: {total_updated:,}", flush=True)

print(f"\nUPDATE concluído: {total_updated:,} registros em {round(time.time()-start)}s")

# ── 5. Limpeza da tabela de mapeamento ───────────────────────────────────────
cur.execute(f'DROP TABLE IF EXISTS "{db_schema}"."_map_motivo";')
conn.commit()
print("Tabela _map_motivo removida.")

# ── 6. Verificação ────────────────────────────────────────────────────────────
cur.execute(f"""
    SELECT motivo_situacao_cadastral, COUNT(*) n
    FROM "{db_schema}"."cnpj_consolidado"
    WHERE motivo_situacao_cadastral IS NOT NULL
      AND motivo_situacao_cadastral NOT IN ('0', '00')
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
""")
print("\nTop motivos (excluindo 00):")
for motivo, n in cur.fetchall():
    print(f"  {motivo}: {n:,}")

cur.close()
conn.close()

os.remove(csv_gz)
print(f"\nArquivo {csv_gz} removido.")
print("Concluído.")
