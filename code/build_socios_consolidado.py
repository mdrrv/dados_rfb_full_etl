"""
build_socios_consolidado.py
Cria dados_rfb.socios_consolidado: socios + dados da empresa (via cnpj_consolidado MATRIZ).

Estratégia zero-downtime: escreve em socios_consolidado_new, swap atômico ao final.
"""
import os, sys, time, pathlib
from io import StringIO
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(pathlib.Path().resolve(), ".env"))
DSN    = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} password={os.getenv('DB_PASSWORD')}"
SCHEMA = os.getenv("DB_SCHEMA", "dados_rfb")
CHUNK  = 500_000

conn = psycopg2.connect(DSN)
cur  = conn.cursor()

# Guardrail
cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".socios')
socios_count = cur.fetchone()[0]
print(f"socios: {socios_count:,} linhas", flush=True)
if socios_count < 1_000_000:
    print("ERRO: socios parece vazio ou incompleto. Abortando.", flush=True)
    sys.exit(1)

cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".cnpj_consolidado')
cc_count = cur.fetchone()[0]
print(f"cnpj_consolidado: {cc_count:,} linhas", flush=True)
if cc_count < 1_000_000:
    print("ERRO: cnpj_consolidado parece vazio. Rode consolidar.py antes. Abortando.", flush=True)
    sys.exit(1)

# Cria _new (sem índices para inserção rápida)
print("Criando socios_consolidado_new...", flush=True)
cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."socios_consolidado_new" CASCADE')
cur.execute(f"""
CREATE TABLE "{SCHEMA}"."socios_consolidado_new" (
    cnpj_basico                      VARCHAR(8),
    identificador_socio              VARCHAR(1),
    nome_socio_razao_social          TEXT,
    cpf_cnpj_socio                   VARCHAR(14),
    qualificacao_socio               VARCHAR(2),
    data_entrada_sociedade           VARCHAR(8),
    pais                             VARCHAR(3),
    faixa_etaria                     VARCHAR(1),
    pessoa_id                        UUID,
    razao_social                     TEXT,
    situacao_cadastral               VARCHAR(2),
    data_situacao_cadastral          VARCHAR(8),
    data_inicio_atividade            VARCHAR(8),
    cnae_fiscal_principal            VARCHAR(7),
    desc_cnae_principal              TEXT,
    uf                               VARCHAR(2),
    nome_municipio                   TEXT,
    porte_empresa                    VARCHAR(2),
    capital_social                   NUMERIC
)
""")
conn.commit()

print("Populando socios_consolidado_new em chunks...", flush=True)
t0 = time.time()
total = 0

cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".socios')
total_socios = cur.fetchone()[0]

conn2 = psycopg2.connect(DSN)
cur2  = conn2.cursor("socios_stream")
cur2.itersize = CHUNK

cur2.execute(f"""
    SELECT
        s.cnpj_basico,
        s.identificador_socio,
        s.nome_socio_razao_social,
        s.cpf_cnpj_socio,
        s.qualificacao_socio,
        s.data_entrada_sociedade,
        s.pais,
        s.faixa_etaria,
        s.pessoa_id,
        c.razao_social,
        c.situacao_cadastral,
        c.data_situacao_cadastral,
        c.data_inicio_atividade,
        c.cnae_fiscal_principal,
        c.desc_cnae_principal,
        c.uf,
        c.nome_municipio,
        c.porte_empresa,
        c.capital_social
    FROM "{SCHEMA}".socios s
    LEFT JOIN LATERAL (
        SELECT razao_social, situacao_cadastral, data_situacao_cadastral,
               data_inicio_atividade, cnae_fiscal_principal, desc_cnae_principal,
               uf, nome_municipio, porte_empresa, capital_social
        FROM "{SCHEMA}".cnpj_consolidado
        WHERE cnpj_basico = s.cnpj_basico
          AND identificador_mf = '1'
        LIMIT 1
    ) c ON true
""")

chunk_num = 0
while True:
    rows = cur2.fetchmany(CHUNK)
    if not rows:
        break
    chunk_num += 1
    buf = StringIO()
    w = csv.writer(buf)
    for row in rows:
        w.writerow(['' if v is None else v for v in row])
    buf.seek(0)
    cur.copy_expert(
        f'COPY "{SCHEMA}"."socios_consolidado_new" FROM STDIN WITH (FORMAT CSV, NULL \'\')',
        buf
    )
    conn.commit()
    total += len(rows)
    pct = round(total / total_socios * 100, 1)
    print(f"  Chunk {chunk_num}: {total:,} / {total_socios:,} ({pct}%)  {round(time.time()-t0)}s", flush=True)

cur2.close()
conn2.close()

# Índices em _new
print("\nCriando índices em socios_consolidado_new...", flush=True)
conn3 = psycopg2.connect(DSN)
conn3.autocommit = True
for name, col in [
    ("idx_sc_new_cnpj_basico", "cnpj_basico"),
    ("idx_sc_new_cpf_cnpj",    "cpf_cnpj_socio"),
    ("idx_sc_new_pessoa_id",   "pessoa_id"),
    ("idx_sc_new_situacao",    "situacao_cadastral"),
]:
    t1 = time.time()
    print(f"  {name}... ", end='', flush=True)
    with conn3.cursor() as c:
        c.execute(f'CREATE INDEX {name} ON "{SCHEMA}"."socios_consolidado_new" ({col})')
    print(f"({round(time.time()-t1)}s)", flush=True)
conn3.close()

with conn.cursor() as c:
    c.execute(f'ANALYZE "{SCHEMA}"."socios_consolidado_new"')
conn.commit()

# Swap atômico
print("\nSwap atômico socios_consolidado_new → socios_consolidado...", flush=True)
with conn.cursor() as c:
    # Se socios_consolidado não existir ainda, apenas renomeia
    c.execute(f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'socios_consolidado'
    """, (SCHEMA,))
    exists = c.fetchone()
    if exists:
        c.execute(f'ALTER TABLE "{SCHEMA}"."socios_consolidado"     RENAME TO "socios_consolidado_old"')
        c.execute(f'ALTER TABLE "{SCHEMA}"."socios_consolidado_new" RENAME TO "socios_consolidado"')
        c.execute(f'DROP TABLE  "{SCHEMA}"."socios_consolidado_old"')
    else:
        c.execute(f'ALTER TABLE "{SCHEMA}"."socios_consolidado_new" RENAME TO "socios_consolidado"')
conn.commit()

cur.close()
conn.close()
print(f"socios_consolidado concluido: {total:,} linhas em {round(time.time()-t0)}s", flush=True)
