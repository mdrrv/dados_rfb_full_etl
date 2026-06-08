"""
build_meili_socios.py
Indexa pessoas_consolidado no Meilisearch (index: pessoas).

Deve rodar APÓS build_pessoas_consolidado.py (última camada do ETL).
Streaming via server-side cursor em chunks de 50k — sem carregar 18M rows em memória.
"""
import os, sys, time, pathlib, psycopg2, meilisearch
from dotenv import load_dotenv

load_dotenv(os.path.join(pathlib.Path().resolve(), ".env"))

DSN    = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} password={os.getenv('DB_PASSWORD')}"
SCHEMA = os.getenv("DB_SCHEMA", "dados_rfb")
MEILI_URL = os.getenv("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
CHUNK = 50_000
INDEX_NAME = "pessoas"

# ── Guardrails ────────────────────────────────────────────────────────────────
conn = psycopg2.connect(DSN)
cur  = conn.cursor()
cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".pessoas_consolidado')
total = cur.fetchone()[0]
print(f"pessoas_consolidado: {total:,} linhas", flush=True)
if total < 1_000_000:
    print("ERRO: pessoas_consolidado parece vazio. Rode build_pessoas_consolidado.py antes.", flush=True)
    sys.exit(1)

# ── Meilisearch: setup do index ───────────────────────────────────────────────
meili = meilisearch.Client(MEILI_URL, MEILI_KEY)

print(f"\nConfigurando Meilisearch index '{INDEX_NAME}'...", flush=True)
try:
    meili.get_index(INDEX_NAME)
    print("  Index existente — será reindexado.", flush=True)
except Exception:
    task = meili.create_index(INDEX_NAME, {"primaryKey": "id"})
    meili.wait_for_task(task.task_uid, timeout_in_ms=30_000)
    print("  Index criado.", flush=True)

idx = meili.index(INDEX_NAME)

# Atributos de busca, filtro e ordenação
idx.update_settings({
    "searchableAttributes": ["nome"],
    "filterableAttributes": ["ativas", "inativas", "score_inativas_pct", "estados_count", "cnaes_count"],
    "sortableAttributes":   ["total_empresas", "ativas", "score_inativas_pct", "anos_experiencia"],
    "rankingRules": [
        "words", "typo", "proximity", "attribute", "sort", "exactness",
        "ativas:desc",
        "total_empresas:desc",
    ],
    "typoTolerance": {
        "enabled": True,
        "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9},
    },
    "pagination": {"maxTotalHits": 10000},
})
print("  Settings atualizados.", flush=True)

# ── Streaming e indexação em chunks ──────────────────────────────────────────
print(f"\nIndexando {total:,} pessoas em chunks de {CHUNK:,}...", flush=True)
t0 = time.time()

conn2 = psycopg2.connect(DSN)
cur2  = conn2.cursor("meili_stream")
cur2.itersize = CHUNK
cur2.execute(f"""
    SELECT
        id::text,
        nome,
        slug,
        total_empresas,
        ativas,
        inativas,
        score_inativas_pct,
        anos_experiencia,
        estados_count,
        cnaes_count
    FROM "{SCHEMA}".pessoas_consolidado
    WHERE nome IS NOT NULL AND nome != ''
    ORDER BY total_empresas DESC
""")

indexed = 0
chunk_num = 0
pending_task = None

while True:
    rows = cur2.fetchmany(CHUNK)
    if not rows:
        break
    chunk_num += 1

    docs = [
        {
            "id":                 r[0],
            "nome":               r[1],
            "slug":               r[2],
            "total_empresas":     r[3] or 0,
            "ativas":             r[4] or 0,
            "inativas":           r[5] or 0,
            "score_inativas_pct": r[6] or 0,
            "anos_experiencia":   r[7],
            "estados_count":      r[8] or 0,
            "cnaes_count":        r[9] or 0,
        }
        for r in rows
    ]

    # Aguarda task anterior antes de enviar próximo chunk (backpressure)
    if pending_task is not None:
        meili.wait_for_task(pending_task, timeout_in_ms=120_000)

    task = idx.add_documents(docs)
    pending_task = task.task_uid
    indexed += len(docs)
    pct = round(indexed / total * 100, 1)
    print(f"  Chunk {chunk_num}: {indexed:,} / {total:,} ({pct}%)  {round(time.time()-t0)}s", flush=True)

# Aguarda último chunk
if pending_task is not None:
    print("\nAguardando indexação final...", flush=True)
    meili.wait_for_task(pending_task, timeout_in_ms=300_000)

cur2.close()
conn2.close()
cur.close()
conn.close()

stats = idx.get_stats()
print(f"\nMeilisearch index '{INDEX_NAME}': {stats.number_of_documents:,} documentos", flush=True)
print(f"Concluído: {indexed:,} pessoas indexadas em {round(time.time()-t0)}s ({round((time.time()-t0)/60)}min)", flush=True)
