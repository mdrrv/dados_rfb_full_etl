"""
ETL TSE — Candidatos Eleitorais (TSE CKAN API)
Baixa dados de candidatos das eleições mais recentes e importa para dados_rfb.tse_candidatos.

Fonte: https://dadosabertos.tse.jus.br/ (CKAN API pública — sem captcha)
Arquivo: https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{ANO}.zip
Ciclo: 2024 (mais recente disponível)

Match na aplicação: por nome (mesmo padrão do PEP) — CPF mascarado nos sócios.
"""
import os, io, csv, zipfile, pathlib, requests, psycopg2, re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path=str(pathlib.Path().resolve() / ".env"))

DB_HOST = os.getenv("DB_HOST", "187.127.13.118")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")
DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

CKAN_BASE   = "https://dadosabertos.tse.jus.br/api/3/action"
CDN_BASE    = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
YEARS       = [2024, 2022, 2020]


DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.tse_candidatos (
    id                  SERIAL PRIMARY KEY,
    ano_eleicao         SMALLINT,
    descricao_eleicao   TEXT,
    nome_candidato      TEXT,
    nome_urna           TEXT,
    cpf                 VARCHAR(14),
    sigla_partido       TEXT,
    descricao_cargo     TEXT,
    sigla_uf            VARCHAR(2),
    nome_municipio      TEXT,
    codigo_situacao     TEXT,
    descricao_situacao  TEXT,
    numero_candidato    TEXT
);

CREATE INDEX IF NOT EXISTS idx_tse_nome  ON dados_rfb.tse_candidatos(nome_candidato);
CREATE INDEX IF NOT EXISTS idx_tse_cpf   ON dados_rfb.tse_candidatos(cpf);
CREATE INDEX IF NOT EXISTS idx_tse_ano   ON dados_rfb.tse_candidatos(ano_eleicao);
"""


def clean_cpf(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits[:11] if len(digits) >= 11 else None


def find_download_url(year: int) -> str:
    # Try CKAN first
    try:
        r = requests.get(f"{CKAN_BASE}/package_show?id=candidatos-{year}", timeout=10)
        if r.ok:
            data = r.json()
            for res in data.get("result", {}).get("resources", []):
                url = res.get("url", "")
                if "consulta_cand" in url and url.endswith(".zip"):
                    return url
    except Exception:
        pass
    # Direct CDN fallback
    return f"{CDN_BASE}/consulta_cand_{year}.zip"


def process_zip(url: str, cur, year: int) -> int:
    print(f"  Download {url} ...", flush=True)
    r = requests.get(url, timeout=180, stream=True)
    r.raise_for_status()
    buf = io.BytesIO(r.content)
    count = 0
    with zipfile.ZipFile(buf) as zf:
        # Use BR-wide file (without UF suffix) or combine all
        names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        # Prefer the consolidated file if it exists
        main_files = [n for n in names if "BRASIL" in n.upper() or n.count("_") <= 3]
        target_files = main_files if main_files else names[:1]  # just first file if no brasil

        for name in target_files:
            print(f"  Processando {name} ...", flush=True)
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            for row in reader:
                k = {kk.strip(): vv.strip() for kk, vv in row.items()}
                nome = (k.get("NM_CANDIDATO") or k.get("NM_URNA_CANDIDATO") or "").upper().strip()
                if not nome:
                    continue
                rows.append((
                    year,
                    k.get("DS_ELEICAO") or None,
                    nome,
                    (k.get("NM_URNA_CANDIDATO") or "").upper().strip() or None,
                    clean_cpf(k.get("NR_CPF_CANDIDATO") or ""),
                    k.get("SG_PARTIDO") or None,
                    k.get("DS_CARGO") or None,
                    (k.get("SG_UF") or "")[:2] or None,
                    k.get("NM_MUNICIPIO") or None,
                    k.get("CD_SIT_TOT_TURNO") or k.get("CD_SITUACAO_CANDIDATURA") or None,
                    k.get("DS_SIT_TOT_TURNO") or k.get("DS_SITUACAO_CANDIDATURA") or None,
                    k.get("NR_CANDIDATO") or None,
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.tse_candidatos
                    (ano_eleicao, descricao_eleicao, nome_candidato, nome_urna, cpf,
                     sigla_partido, descricao_cargo, sigla_uf, nome_municipio,
                     codigo_situacao, descricao_situacao, numero_candidato)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} candidatos de {name}", flush=True)
            break  # one file is enough for the consolidated national data
    return count


def main():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: tse_candidatos ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    print("=== Truncando tabela ===", flush=True)
    cur.execute("TRUNCATE dados_rfb.tse_candidatos RESTART IDENTITY")
    conn.commit()

    total = 0
    for year in YEARS:
        print(f"\n=== Candidatos {year} ===", flush=True)
        try:
            url = find_download_url(year)
            n = process_zip(url, cur, year)
            conn.commit()
            total += n
            print(f"  {year}: {n:,} candidatos importados", flush=True)
        except Exception as e:
            print(f"  ERRO {year}: {e}", flush=True)
            conn.rollback()

    print("\n=== ANALYZE ===", flush=True)
    cur.execute("ANALYZE dados_rfb.tse_candidatos")
    conn.commit()
    print(f"\n=== CONCLUÍDO: {total:,} candidatos ===", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
