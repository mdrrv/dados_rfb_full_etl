"""
consolidar_fast.py — rebuild de cnpj_consolidado sem indexes durante o INSERT.

Diferenças do consolidar.py original:
- Dropa TODOS os 23 índices antes de inserir (inclui UNIQUE)
- INSERT simples sem ON CONFLICT (sem índice único não há como verificar conflito)
- Recria todos os índices ao final em ordem: btree primeiro, GIN depois

Resultado esperado: ~95% mais rápido na fase de inserção (15h → 30-60min).
"""
import os
import pathlib
import sys
import time

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

conn = psycopg2.connect(DSN)
conn.autocommit = False

# ── 1. DROP todos os índices de cnpj_consolidado ──────────────────────────────
DROP_INDEXES = [
    "cnpj_consolidado_basico",
    "cnpj_consolidado_cnpj",
    "cnpj_consolidado_email",
    "cnpj_consolidado_endereco",
    "cnpj_consolidado_fantasia_trgm",
    "cnpj_consolidado_razao",
    "cnpj_consolidado_razao_trgm",
    "cnpj_consolidado_sit",
    "cnpj_consolidado_uf",
    "cnpj_consolidado_uf_mun",
    "idx_cnpj_consolidado_cnpj_basico",
    "idx_cnpj_consolidado_razao",
    "idx_cnpj_consolidado_razao_trgm",
    "idx_consolidado_cep",
    "idx_consolidado_cnae",
    "idx_consolidado_email",
    "idx_fantasia_trgm",
    "idx_fantasia_unaccent_trgm",
    "idx_fts_simple",
    "idx_fts_simple_ativa",
    "idx_razao_social_btree",
    "idx_razao_trgm",
    "idx_razao_unaccent_trgm",
]

print("=== FASE 1: Dropando índices ===", flush=True)
with conn.cursor() as c:
    for idx in DROP_INDEXES:
        c.execute(f'DROP INDEX IF EXISTS "{db_schema}"."{idx}";')
        print(f"  DROP {idx}", flush=True)
conn.commit()
print(f"  {len(DROP_INDEXES)} índices removidos.\n", flush=True)

# ── 2. Staging: cria cnpj_consolidado_new (sem truncar a tabela ativa) ────────
print("=== FASE 2: Criando staging cnpj_consolidado_new ===", flush=True)
with conn.cursor() as c:
    c.execute(f'DROP TABLE IF EXISTS "{db_schema}"."cnpj_consolidado_new";')
    c.execute(f'CREATE TABLE "{db_schema}"."cnpj_consolidado_new" (LIKE "{db_schema}"."cnpj_consolidado" INCLUDING DEFAULTS);')
conn.commit()
print("  Staging criada. cnpj_consolidado segue ativa com dados antigos.\n", flush=True)

# ── 3. Verifica índice de suporte em estabelecimento ─────────────────────────
print("=== FASE 3: Verificando índice em estabelecimento ===", flush=True)
with conn.cursor() as c:
    c.execute("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname = %s AND tablename = 'estabelecimento'
        AND indexdef LIKE '%%cnpj_basico%%' LIMIT 1;
    """, (db_schema,))
    idx = c.fetchone()
    if not idx:
        print("  Criando idx_estab_cnpj_basico...", flush=True)
        conn.autocommit = True
        with conn.cursor() as c2:
            c2.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_cnpj_basico ON "{db_schema}"."estabelecimento" (cnpj_basico);')
        conn.autocommit = False
        print("  Índice criado.", flush=True)
    else:
        print(f"  OK: {idx[0]}", flush=True)

with conn.cursor() as c:
    c.execute("SET work_mem = '512MB';")
conn.commit()
print("  work_mem = 512MB\n", flush=True)

# ── 4. INSERT em 100 faixas (sem ON CONFLICT) ─────────────────────────────────
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

SELECT_SQL = f"""
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

INSERT_SQL = f'INSERT INTO "{db_schema}"."cnpj_consolidado_new" ({col_list}) {SELECT_SQL};'

print("=== FASE 4: Inserindo 100 faixas (sem índices) ===", flush=True)
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
    elapsed = round(time.time() - t0)
    print(f"{n:,} inseridos ({elapsed}s) — total: {total_inserted:,}", flush=True)

total_secs = round(time.time() - start)
print(f"\n  TOTAL: {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)\n", flush=True)

# ── 5. ANALYZE antes dos índices ──────────────────────────────────────────────
print("=== FASE 5: ANALYZE ===", flush=True)
with conn.cursor() as c:
    c.execute(f'ANALYZE "{db_schema}"."cnpj_consolidado_new";')
conn.commit()
print("  Estatísticas atualizadas.\n", flush=True)

# ── 6. Recriar TODOS os 23 índices ───────────────────────────────────────────
# CREATE INDEX CONCURRENTLY requer autocommit = True
conn.close()
conn2 = psycopg2.connect(DSN)
conn2.autocommit = True

INDEX_DDLS = [
    # --- btree simples (rápidos) ---
    f'CREATE UNIQUE INDEX IF NOT EXISTS cnpj_consolidado_cnpj ON "{db_schema}"."cnpj_consolidado_new" USING btree (cnpj)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_basico ON "{db_schema}"."cnpj_consolidado_new" USING btree (cnpj_basico)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_sit ON "{db_schema}"."cnpj_consolidado_new" USING btree (situacao_cadastral)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_uf ON "{db_schema}"."cnpj_consolidado_new" USING btree (uf)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_uf_mun ON "{db_schema}"."cnpj_consolidado_new" USING btree (uf, nome_municipio)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_razao ON "{db_schema}"."cnpj_consolidado_new" USING btree (razao_social)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_endereco ON "{db_schema}"."cnpj_consolidado_new" USING btree (cep, logradouro, numero)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_email ON "{db_schema}"."cnpj_consolidado_new" USING btree (correio_eletronico) WHERE (correio_eletronico IS NOT NULL)',
    f'CREATE INDEX IF NOT EXISTS idx_cnpj_consolidado_cnpj_basico ON "{db_schema}"."cnpj_consolidado_new" USING btree (cnpj_basico)',
    f'CREATE INDEX IF NOT EXISTS idx_consolidado_cep ON "{db_schema}"."cnpj_consolidado_new" USING btree (cep)',
    f'CREATE INDEX IF NOT EXISTS idx_consolidado_cnae ON "{db_schema}"."cnpj_consolidado_new" USING btree (cnae_fiscal_principal)',
    f'CREATE INDEX IF NOT EXISTS idx_consolidado_email ON "{db_schema}"."cnpj_consolidado_new" USING btree (correio_eletronico)',
    f'CREATE INDEX IF NOT EXISTS idx_razao_social_btree ON "{db_schema}"."cnpj_consolidado_new" USING btree (razao_social)',
    # --- GIN trgm (lentos — 20-60min cada) ---
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_razao_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(razao_social) gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS cnpj_consolidado_fantasia_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(nome_fantasia) gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS idx_cnpj_consolidado_razao_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (razao_social gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS idx_fantasia_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(nome_fantasia) gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS idx_fantasia_unaccent_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(nome_fantasia) gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS idx_razao_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(razao_social) gin_trgm_ops)',
    f'CREATE INDEX IF NOT EXISTS idx_razao_unaccent_trgm ON "{db_schema}"."cnpj_consolidado_new" USING gin (immutable_unaccent(razao_social) gin_trgm_ops)',
    # --- GIN FTS (lentos) ---
    f"""CREATE INDEX IF NOT EXISTS idx_cnpj_consolidado_razao ON "{db_schema}"."cnpj_consolidado_new" USING gin (to_tsvector('simple'::regconfig, ((COALESCE(razao_social, ''::text) || ' '::text) || COALESCE(nome_fantasia, ''::text))))""",
    f"""CREATE INDEX IF NOT EXISTS idx_fts_simple ON "{db_schema}"."cnpj_consolidado_new" USING gin (to_tsvector('simple'::regconfig, ((immutable_unaccent(COALESCE(razao_social, ''::text)) || ' '::text) || immutable_unaccent(COALESCE(nome_fantasia, ''::text)))))""",
    f"""CREATE INDEX IF NOT EXISTS idx_fts_simple_ativa ON "{db_schema}"."cnpj_consolidado_new" USING gin (to_tsvector('simple'::regconfig, ((immutable_unaccent(COALESCE(razao_social, ''::text)) || ' '::text) || immutable_unaccent(COALESCE(nome_fantasia, ''::text))))) WHERE ((situacao_cadastral)::text = '02'::text)""",
]

print(f"=== FASE 6: Recriando {len(INDEX_DDLS)} índices ===", flush=True)
for ddl in INDEX_DDLS:
    name = ddl.split('INDEX IF NOT EXISTS ')[1].split(' ')[0]
    print(f"  {name}... ", end='', flush=True)
    t0 = time.time()
    with conn2.cursor() as c:
        c.execute(ddl)
    print(f"ok ({round(time.time()-t0)}s)", flush=True)

conn2.close()

# ── 7. Swap atômico: cnpj_consolidado_new → cnpj_consolidado ─────────────────
print("\n=== FASE 7: Swap atômico (RENAME) ===", flush=True)
conn3 = psycopg2.connect(DSN)
conn3.autocommit = False
with conn3.cursor() as c:
    # Se cnpj_consolidado_old sobrou de run anterior, limpar primeiro
    c.execute(f'DROP TABLE IF EXISTS "{db_schema}"."cnpj_consolidado_old";')
    conn3.commit()
    # Se cnpj_consolidado existe (run normal), renomear para _old; senão (primeiro run), skip
    c.execute(f"""
        SELECT EXISTS (
            SELECT FROM pg_class WHERE relname = 'cnpj_consolidado'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{db_schema}')
        )
    """)
    if c.fetchone()[0]:
        c.execute(f'ALTER TABLE "{db_schema}"."cnpj_consolidado" RENAME TO "cnpj_consolidado_old";')
    c.execute(f'ALTER TABLE "{db_schema}"."cnpj_consolidado_new" RENAME TO "cnpj_consolidado";')
conn3.commit()
print("  cnpj_consolidado agora aponta para os dados novos.", flush=True)
with conn3.cursor() as c:
    c.execute(f'DROP TABLE IF EXISTS "{db_schema}"."cnpj_consolidado_old";')
conn3.commit()
conn3.close()
print("  Tabela antiga removida.\n", flush=True)

print(f"=== CONCLUIDO: cnpj_consolidado reconstruida com {total_inserted:,} registros e {len(INDEX_DDLS)} indices (zero-downtime) ===", flush=True)
