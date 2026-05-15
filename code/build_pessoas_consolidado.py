"""
build_pessoas_consolidado.py
Pre-calcula stats por pessoa (score, contagens, anos de experiencia).

Estratégia zero-downtime: escreve em pessoas_consolidado_new, swap atômico ao final.
"""
import os, sys, time, pathlib
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(pathlib.Path().resolve(), ".env"))
DSN    = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} password={os.getenv('DB_PASSWORD')}"
SCHEMA = os.getenv("DB_SCHEMA", "dados_rfb")

conn = psycopg2.connect(DSN)
cur  = conn.cursor()

# Guardrail
cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".socios_consolidado')
sc_count = cur.fetchone()[0]
print(f"socios_consolidado: {sc_count:,} linhas", flush=True)
if sc_count < 1_000_000:
    print("ERRO: socios_consolidado parece vazio. Rode build_socios_consolidado.py antes.", flush=True)
    sys.exit(1)

# Cria _new
print("Criando pessoas_consolidado_new...", flush=True)
cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."pessoas_consolidado_new" CASCADE')
cur.execute(f"""
CREATE TABLE "{SCHEMA}"."pessoas_consolidado_new" (
    id                  UUID        PRIMARY KEY,
    cpf_cnpj            TEXT        NOT NULL,
    nome                TEXT        NOT NULL,
    slug                TEXT        NOT NULL UNIQUE,
    total_empresas      INTEGER     DEFAULT 0,
    ativas              INTEGER     DEFAULT 0,
    inativas            INTEGER     DEFAULT 0,
    score_inativas_pct  INTEGER     DEFAULT 0,
    ano_primeira_entrada INTEGER,
    anos_experiencia    INTEGER,
    estados_count       INTEGER     DEFAULT 0,
    cnaes_count         INTEGER     DEFAULT 0,
    updated_at          TIMESTAMP   DEFAULT NOW()
)
""")
conn.commit()

print("Populando pessoas_consolidado_new (agregação via SQL)...", flush=True)
t0 = time.time()

cur.execute(f"""
INSERT INTO "{SCHEMA}"."pessoas_consolidado_new"
    (id, cpf_cnpj, nome, slug,
     total_empresas, ativas, inativas, score_inativas_pct,
     ano_primeira_entrada, anos_experiencia,
     estados_count, cnaes_count, updated_at)
SELECT
    p.id,
    p.cpf_cnpj,
    p.nome,
    p.slug,
    COUNT(DISTINCT sc.cnpj_basico)                                          AS total_empresas,
    COUNT(DISTINCT CASE WHEN sc.situacao_cadastral IN ('2','02') THEN sc.cnpj_basico END) AS ativas,
    COUNT(DISTINCT CASE WHEN sc.situacao_cadastral NOT IN ('2','02') THEN sc.cnpj_basico END) AS inativas,
    CASE
        WHEN COUNT(DISTINCT sc.cnpj_basico) > 0
        THEN ROUND(
            COUNT(DISTINCT CASE WHEN sc.situacao_cadastral NOT IN ('2','02') THEN sc.cnpj_basico END)::numeric
            / COUNT(DISTINCT sc.cnpj_basico) * 100
        )::integer
        ELSE 0
    END                                                                     AS score_inativas_pct,
    MIN(CASE
        WHEN sc.data_entrada_sociedade ~ '^\d{{8}}$'
         AND LEFT(sc.data_entrada_sociedade, 4)::integer BETWEEN 1950 AND 2030
        THEN LEFT(sc.data_entrada_sociedade, 4)::integer
    END)                                                                    AS ano_primeira_entrada,
    CASE
        WHEN MIN(CASE
            WHEN sc.data_entrada_sociedade ~ '^\d{{8}}$'
             AND LEFT(sc.data_entrada_sociedade, 4)::integer BETWEEN 1950 AND 2030
            THEN LEFT(sc.data_entrada_sociedade, 4)::integer
        END) IS NOT NULL
        THEN EXTRACT(YEAR FROM NOW())::integer - MIN(CASE
            WHEN sc.data_entrada_sociedade ~ '^\d{{8}}$'
             AND LEFT(sc.data_entrada_sociedade, 4)::integer BETWEEN 1950 AND 2030
            THEN LEFT(sc.data_entrada_sociedade, 4)::integer
        END)
    END                                                                     AS anos_experiencia,
    COUNT(DISTINCT sc.uf)                                                   AS estados_count,
    COUNT(DISTINCT sc.cnae_fiscal_principal)                                AS cnaes_count,
    NOW()
FROM "{SCHEMA}".pessoas p
JOIN "{SCHEMA}".socios_consolidado sc
  ON sc.cpf_cnpj_socio = p.cpf_cnpj
 AND UPPER(TRIM(sc.nome_socio_razao_social)) = p.nome
GROUP BY p.id, p.cpf_cnpj, p.nome, p.slug
""")
conn.commit()

cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".pessoas_consolidado_new')
total = cur.fetchone()[0]
print(f"  {total:,} pessoas inseridas em {round(time.time()-t0)}s", flush=True)

# Índices em _new
print("Criando índices...", flush=True)
for sql in [
    f'CREATE INDEX idx_pc_new_cpf_cnpj ON "{SCHEMA}".pessoas_consolidado_new (cpf_cnpj)',
    f'CREATE INDEX idx_pc_new_slug     ON "{SCHEMA}".pessoas_consolidado_new (slug)',
    f'CREATE INDEX idx_pc_new_score    ON "{SCHEMA}".pessoas_consolidado_new (score_inativas_pct)',
]:
    cur.execute(sql)
conn.commit()

with conn.cursor() as c:
    c.execute(f'ANALYZE "{SCHEMA}"."pessoas_consolidado_new"')
conn.commit()

# Swap atômico
print("Swap atômico pessoas_consolidado_new → pessoas_consolidado...", flush=True)
with conn.cursor() as c:
    c.execute(f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'pessoas_consolidado'
    """, (SCHEMA,))
    exists = c.fetchone()
    if exists:
        c.execute(f'ALTER TABLE "{SCHEMA}"."pessoas_consolidado"     RENAME TO "pessoas_consolidado_old"')
        c.execute(f'ALTER TABLE "{SCHEMA}"."pessoas_consolidado_new" RENAME TO "pessoas_consolidado"')
        c.execute(f'DROP TABLE  "{SCHEMA}"."pessoas_consolidado_old"')
    else:
        c.execute(f'ALTER TABLE "{SCHEMA}"."pessoas_consolidado_new" RENAME TO "pessoas_consolidado"')
conn.commit()

cur.close()
conn.close()
print(f"pessoas_consolidado concluido: {total:,} linhas em {round(time.time()-t0)}s", flush=True)
