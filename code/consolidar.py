"""
Script standalone: popula cnpj_consolidado em chunks por faixa de cnpj_basico.

Estratégia: sem carregar empresa/simples na RAM.
- Divide estabelecimento em 100 faixas pelos dois primeiros dígitos de cnpj_basico (00 a 99)
- Para cada faixa: PostgreSQL faz o JOIN com índices, COPY TO STDOUT → Python
- Python grava no cnpj_consolidado via COPY FROM STDIN

Uso:
    cd /root/dados_rfb_full_etl
    source /root/etl_venv/bin/activate
    nohup python -u code/consolidar.py > /root/consolidar.log 2>&1 &
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

# ── conexões ─────────────────────────────────────────────────────────────────
conn = psycopg2.connect(DSN)

# ── truncate ─────────────────────────────────────────────────────────────────
print("Truncando cnpj_consolidado...", flush=True)
with conn.cursor() as c:
    c.execute(f'TRUNCATE TABLE "{db_schema}"."cnpj_consolidado";')
conn.commit()

# ── verifica índice em estabelecimento ───────────────────────────────────────
print("Verificando índice em estabelecimento.cnpj_basico...", flush=True)
with conn.cursor() as c:
    c.execute("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname = %s AND tablename = 'estabelecimento'
        AND indexdef LIKE '%%cnpj_basico%%'
        LIMIT 1;
    """, (db_schema,))
    idx = c.fetchone()
    if not idx:
        print("  Criando índice (pode levar alguns minutos)...", flush=True)
        t0 = time.time()
        c.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_cnpj_basico ON "{db_schema}"."estabelecimento" (cnpj_basico);')
        conn.commit()
        print(f"  Índice criado em {round(time.time()-t0)}s", flush=True)
    else:
        print(f"  Índice encontrado: {idx[0]}", flush=True)

# ── Aumento temporário de memória no DB para os JOINs ────────────────────────
print("Ajustando work_mem da conexão para acelerar os joins...", flush=True)
with conn.cursor() as c:
    c.execute("SET work_mem = '256MB';")
conn.commit()

# ── JOIN por faixas de cnpj_basico ────────────────────────────────────────────

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

COPY_SELECT = f"""
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

INSERT_SQL = f"""
    INSERT INTO "{db_schema}"."cnpj_consolidado" ({col_list})
    {COPY_SELECT}
    ON CONFLICT (cnpj) DO NOTHING;
"""

print("\nIniciando consolidação em 100 pedaços (00 a 99)...", flush=True)
total_inserted = 0
start = time.time()

for digit in range(100):
    t0 = time.time()
    
    # FORMATADOR MÁGICO: Transforma 0 em "00", 1 em "01", etc.
    digit_str = f"{digit:02d}" 
    
    print(f"  Faixa {digit_str}... ", end='', flush=True)

    with conn.cursor() as c:
        c.execute(
            INSERT_SQL.replace('{digit}', digit_str)
        )
        n = c.rowcount
        conn.commit()

    total_inserted += n
    print(f"{n:,} inseridos ({round(time.time()-t0)}s) — total: {total_inserted:,}", flush=True)

total_secs = round(time.time() - start)
print(f"\ncnpj_consolidado populado! {total_inserted:,} registros em {total_secs}s ({round(total_secs/60)}min)")

# ── analyze ──────────────────────────────────────────────────────────────────
print("\nAtualizando estatísticas do Postgres (ANALYZE)...", flush=True)
with conn.cursor() as c:
    c.execute(f'ANALYZE "{db_schema}"."cnpj_consolidado";')
conn.commit()

# ── índices pós-consolidação ──────────────────────────────────────────────────
INDEXES = [
    ("idx_cnpj_consolidado_cnpj_basico", "cnpj_consolidado", "cnpj_basico"),
    ("idx_cnpj_consolidado_razao_trgm",  None,               None),          # trigrama — criado separadamente
    ("idx_consolidado_cep",              "cnpj_consolidado", "cep"),
    ("idx_consolidado_email",            "cnpj_consolidado", "correio_eletronico"),
    ("idx_consolidado_cnae",             "cnpj_consolidado", "cnae_fiscal_principal"),
    ("idx_socios_cpf_cnpj",              "socios",           "cpf_cnpj_socio"),
]

conn2 = psycopg2.connect(DSN)
conn2.autocommit = True  # CREATE INDEX não pode rodar dentro de transação

print("\nVerificando índices...", flush=True)
for idx_name, table, column in INDEXES:
    if table is None:
        with conn2.cursor() as c:
            c.execute("SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s", (db_schema, idx_name))
            if not c.fetchone():
                print(f"  AVISO: {idx_name} não existe — crie manualmente com pg_trgm extension.", flush=True)
            else:
                print(f"  OK: {idx_name}", flush=True)
        continue

    with conn2.cursor() as c:
        c.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND tablename = %s AND indexname = %s",
            (db_schema, table, idx_name)
        )
        exists = c.fetchone()

    if exists:
        print(f"  OK: {idx_name}", flush=True)
    else:
        print(f"  Criando {idx_name} em {table}.{column}...", end=' ', flush=True)
        t0 = time.time()
        with conn2.cursor() as c:
            c.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON "{db_schema}"."{table}" ({column});')
        print(f"({round(time.time()-t0)}s)", flush=True)

conn2.close()
print("\nProcesso Finalizado com Sucesso!", flush=True)