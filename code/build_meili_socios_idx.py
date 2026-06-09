"""
build_meili_socios_idx.py
Indexa socios_consolidado no Meilisearch (index: socios).

27M docs — cada doc representa uma relação socio<>empresa.
Searchable por nome. Filterable por cnpj_basico, situacao_cadastral, uf,
porte_empresa, identificador_socio. Útil para:
  - Busca de sócios por nome (empresa page overlay / future)
  - Lookup de todas as empresas de um CPF/CNPJ (filtro cpf_cnpj_socio)

Deve rodar APÓS build_socios_consolidado.py.
"""
import os, sys, time, pathlib, psycopg2, meilisearch
from dotenv import load_dotenv

load_dotenv(os.path.join(pathlib.Path().resolve(), ".env"))

DSN       = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} password={os.getenv('DB_PASSWORD')}"
SCHEMA    = os.getenv("DB_SCHEMA", "dados_rfb")
MEILI_URL = os.getenv("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
CHUNK     = 50_000
INDEX_NAME = "socios"

conn = psycopg2.connect(DSN)
cur  = conn.cursor()
cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".socios_consolidado')
total = cur.fetchone()[0]
print(f"socios_consolidado: {total:,} linhas", flush=True)
if total < 1_000_000:
    print("ERRO: socios_consolidado parece vazio. Rode build_socios_consolidado.py antes.", flush=True)
    sys.exit(1)
cur.close(); conn.close()

meili = meilisearch.Client(MEILI_URL, MEILI_KEY)

print(f"\nConfigurando index '{INDEX_NAME}'...", flush=True)
try:
    meili.get_index(INDEX_NAME)
    print("  Index existente.", flush=True)
except Exception:
    task = meili.create_index(INDEX_NAME, {"primaryKey": "row_id"})
    meili.wait_for_task(task.task_uid, timeout_in_ms=30_000)
    print("  Index criado.", flush=True)

idx = meili.index(INDEX_NAME)

already_indexed = 0
try:
    already_indexed = idx.get_stats().number_of_documents
except Exception:
    pass
if already_indexed > 0:
    print(f"  Resumindo: {already_indexed:,} docs já indexados.", flush=True)

idx.update_settings({
    "searchableAttributes": ["nome_socio_razao_social"],
    "filterableAttributes": [
        "cnpj_basico", "cpf_cnpj_socio", "situacao_cadastral",
        "uf", "porte_empresa", "identificador_socio", "qualificacao_socio",
    ],
    "sortableAttributes": ["data_entrada_sociedade", "capital_social"],
    "typoTolerance": {
        "enabled": True,
        "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9},
    },
    "pagination": {"maxTotalHits": 10000},
})
print("  Settings atualizados.", flush=True)

print(f"\nIndexando {total:,} socios em chunks de {CHUNK:,}...", flush=True)
t0 = time.time()

conn2 = psycopg2.connect(DSN)
cur2  = conn2.cursor("meili_socios_stream")
cur2.itersize = CHUNK
offset_clause = f"OFFSET {already_indexed}" if already_indexed > 0 else ""
cur2.execute(f"""
    SELECT
        ROW_NUMBER() OVER (ORDER BY cnpj_basico, cpf_cnpj_socio) AS row_id,
        cnpj_basico,
        nome_socio_razao_social,
        cpf_cnpj_socio,
        qualificacao_socio,
        data_entrada_sociedade,
        situacao_cadastral,
        uf,
        porte_empresa,
        capital_social,
        identificador_socio,
        razao_social,
        cnae_fiscal_principal,
        desc_cnae_principal
    FROM "{SCHEMA}".socios_consolidado
    ORDER BY cnpj_basico, cpf_cnpj_socio
    {offset_clause}
""")

indexed = already_indexed
chunk_num = 0
pending_task = None

while True:
    rows = cur2.fetchmany(CHUNK)
    if not rows:
        break
    chunk_num += 1

    docs = [
        {
            "row_id":                 r[0],
            "cnpj_basico":            r[1],
            "nome_socio_razao_social": r[2],
            "cpf_cnpj_socio":         r[3],
            "qualificacao_socio":     r[4],
            "data_entrada_sociedade": r[5],
            "situacao_cadastral":     r[6],
            "uf":                     r[7],
            "porte_empresa":          r[8],
            "capital_social":         float(r[9]) if r[9] else 0.0,
            "identificador_socio":    r[10],
            "razao_social":           r[11],
            "cnae_fiscal_principal":  r[12],
            "desc_cnae_principal":    r[13],
        }
        for r in rows
    ]

    if pending_task is not None:
        meili.wait_for_task(pending_task, timeout_in_ms=120_000)

    task = idx.add_documents(docs)
    pending_task = task.task_uid
    indexed += len(docs)
    pct = round(indexed / total * 100, 1)
    elapsed = round(time.time() - t0)
    print(f"  Chunk {chunk_num}: {indexed:,} / {total:,} ({pct}%)  {elapsed}s", flush=True)

if pending_task is not None:
    print("\nAguardando indexação final...", flush=True)
    meili.wait_for_task(pending_task, timeout_in_ms=300_000)

cur2.close(); conn2.close()

stats = idx.get_stats()
print(f"\nMeilisearch index '{INDEX_NAME}': {stats.number_of_documents:,} documentos", flush=True)
print(f"Concluído: {indexed:,} socios indexados em {round(time.time()-t0)}s", flush=True)
