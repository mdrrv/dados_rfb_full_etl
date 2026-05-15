"""
ETL PEP — Pessoas Expostas Politicamente (Portal da Transparência / CGU)
Baixa CSV e importa para dados_rfb.pep.
Fonte: https://portaldatransparencia.gov.br/download-de-dados/pep
"""
import os, io, csv, zipfile, re, requests, psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "rfb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/pep"


def find_latest_url() -> tuple[str, str]:
    today = datetime.today()
    for delta in range(0, 6):
        from datetime import timedelta
        dt = today - timedelta(days=delta * 30)
        ym = dt.strftime("%Y%m")
        url = f"{BASE_URL}/{ym}_PEP.zip"
        r = requests.head(url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return url, ym
    # Fallback: try without year prefix
    url = f"{BASE_URL}/PEP.zip"
    r = requests.head(url, timeout=10, allow_redirects=True)
    if r.status_code == 200:
        return url, "latest"
    raise RuntimeError("Nenhum arquivo PEP encontrado")


DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.pep (
    id              SERIAL PRIMARY KEY,
    cpf             VARCHAR(14),
    nome            TEXT,
    sigla_funcao    TEXT,
    descricao_funcao TEXT,
    nivel_funcao    TEXT,
    nome_orgao      TEXT,
    sigla_uf        VARCHAR(2),
    data_inicio_exercicio DATE,
    data_fim_exercicio    DATE,
    data_fim_carencia     DATE,
    em_exercicio    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_pep_cpf  ON dados_rfb.pep(cpf);
CREATE INDEX IF NOT EXISTS idx_pep_nome ON dados_rfb.pep(nome);
CREATE INDEX IF NOT EXISTS idx_pep_exercicio ON dados_rfb.pep(em_exercicio) WHERE em_exercicio;
"""


def clean_cpf(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits[:11] if len(digits) >= 11 else None


def parse_date(s: str) -> "str | None":
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def process_zip(url: str, cur) -> int:
    print(f"  Download {url} ...", flush=True)
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    buf = io.BytesIO(r.content)

    count = 0
    with zipfile.ZipFile(buf) as zf:
        for name in zf.namelist():
            if not name.upper().endswith(".CSV"):
                continue
            print(f"  Processando {name} ...", flush=True)
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            today = datetime.today().date()
            for row in reader:
                keys = {k.strip().lower(): v.strip() for k, v in row.items()}
                data_fim_raw = keys.get("data de fim do exercício") or keys.get("data fim exercicio") or ""
                data_fim     = parse_date(data_fim_raw)
                data_carencia_raw = keys.get("data de fim da carência") or keys.get("data fim carencia") or ""
                data_carencia = parse_date(data_carencia_raw)

                em_exercicio = not bool(data_fim)
                if data_carencia:
                    from datetime import date as date_cls
                    try:
                        em_exercicio = date_cls.fromisoformat(data_carencia) >= today
                    except Exception:
                        pass

                rows.append((
                    clean_cpf(keys.get("cpf da pessoa exposta politicamente") or keys.get("cpf") or ""),
                    keys.get("nome da pessoa exposta politicamente") or keys.get("nome") or None,
                    keys.get("sigla da função") or keys.get("sigla funcao") or None,
                    keys.get("descrição da função") or keys.get("descricao funcao") or None,
                    keys.get("nível da função") or keys.get("nivel funcao") or None,
                    keys.get("nome do órgão") or keys.get("nome orgao") or None,
                    (keys.get("sigla da uf") or keys.get("sigla uf") or "")[:2] or None,
                    parse_date(keys.get("data de início do exercício") or keys.get("data inicio exercicio") or ""),
                    data_fim,
                    data_carencia,
                    em_exercicio,
                ))

            cur.executemany("""
                INSERT INTO dados_rfb.pep
                    (cpf, nome, sigla_funcao, descricao_funcao, nivel_funcao,
                     nome_orgao, sigla_uf, data_inicio_exercicio, data_fim_exercicio,
                     data_fim_carencia, em_exercicio)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} registros inseridos de {name}", flush=True)
    return count


def main():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: pep ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    print("=== Truncando tabela ===", flush=True)
    cur.execute("TRUNCATE dados_rfb.pep RESTART IDENTITY")
    conn.commit()

    try:
        url, ym = find_latest_url()
        print(f"Arquivo encontrado: {ym}", flush=True)
        n = process_zip(url, cur)
        conn.commit()
        print(f"\n=== ANALYZE ===", flush=True)
        cur.execute("ANALYZE dados_rfb.pep")
        conn.commit()
        print(f"\n=== CONCLUÍDO: {n:,} PEPs importados ===", flush=True)
    except Exception as e:
        print(f"ERRO: {e}", flush=True)
        conn.rollback()

    conn.close()


if __name__ == "__main__":
    main()
