"""
ETL CEPIM + Acordos de Leniência — Portal da Transparência (CGU)
CEPIM: entidades privadas impedidas de receber repasses federais
Leniência: empresas com acordo de leniência firmado

Fontes:
  CEPIM:     https://portaldatransparencia.gov.br/download-de-dados/cepim
  Leniência: https://portaldatransparencia.gov.br/download-de-dados/acordos-leniencia
"""
import os, io, csv, zipfile, re, requests, psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "rfb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

DATASETS = {
    "cepim":            ("CEPIM",         "cepim"),
    "acordos-leniencia":("ACORDOS_LENIENCIA", "leniencia"),
}


def find_url(endpoint: str, label: str) -> tuple[str, str]:
    base = f"https://portaldatransparencia.gov.br/download-de-dados/{endpoint}"
    today = datetime.today()
    for delta in range(0, 6):
        dt = today - timedelta(days=delta * 30)
        ym = dt.strftime("%Y%m")
        url = f"{base}/{ym}_{label.upper()}.zip"
        r = requests.head(url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return url, ym
    # Try without year prefix
    for name in [f"{label.upper()}.zip", f"{label.lower()}.zip"]:
        url = f"{base}/{name}"
        r = requests.head(url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return url, "latest"
    raise RuntimeError(f"Arquivo {label} não encontrado")


DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.cepim (
    id              SERIAL PRIMARY KEY,
    cnpj_14         VARCHAR(14),
    nome_entidade   TEXT,
    convenio        TEXT,
    orgao_concedente TEXT,
    motivo_impedimento TEXT,
    data_impedimento DATE,
    data_fim_impedimento DATE,
    ativo           BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_cepim_cnpj ON dados_rfb.cepim(cnpj_14);
CREATE INDEX IF NOT EXISTS idx_cepim_ativo ON dados_rfb.cepim(ativo) WHERE ativo;

CREATE TABLE IF NOT EXISTS dados_rfb.acordos_leniencia (
    id              SERIAL PRIMARY KEY,
    cnpj_14         VARCHAR(14),
    nome_empresa    TEXT,
    data_assinatura DATE,
    data_publicacao DATE,
    orgao_acordo    TEXT,
    situacao        TEXT,
    descricao       TEXT
);
CREATE INDEX IF NOT EXISTS idx_leniencia_cnpj ON dados_rfb.acordos_leniencia(cnpj_14);
"""


def clean_cnpj(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits[:14] if len(digits) >= 14 else None


def parse_date(s: str) -> "str | None":
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def process_cepim(url: str, cur) -> int:
    print(f"  Download CEPIM {url} ...", flush=True)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    count = 0
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        for name in zf.namelist():
            if not name.upper().endswith(".CSV"):
                continue
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            today = datetime.today().date()
            for row in reader:
                k = {kk.strip().lower(): vv.strip() for kk, vv in row.items()}
                data_fim_raw = k.get("data fim impedimento") or k.get("data de fim do impedimento") or ""
                data_fim = parse_date(data_fim_raw)
                ativo = not bool(data_fim)
                if data_fim:
                    try:
                        from datetime import date as d_cls
                        ativo = d_cls.fromisoformat(data_fim) >= today
                    except Exception:
                        pass
                rows.append((
                    clean_cnpj(k.get("cnpj") or ""),
                    k.get("nome da entidade") or k.get("nome entidade") or None,
                    k.get("número do convênio") or k.get("convenio") or None,
                    k.get("órgão concedente") or k.get("orgao concedente") or None,
                    k.get("motivo do impedimento") or k.get("motivo impedimento") or None,
                    parse_date(k.get("data do impedimento") or ""),
                    data_fim,
                    ativo,
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.cepim
                    (cnpj_14, nome_entidade, convenio, orgao_concedente,
                     motivo_impedimento, data_impedimento, data_fim_impedimento, ativo)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} CEPIM inseridos", flush=True)
    return count


def process_leniencia(url: str, cur) -> int:
    print(f"  Download Leniência {url} ...", flush=True)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    count = 0
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        for name in zf.namelist():
            if not name.upper().endswith(".CSV"):
                continue
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            for row in reader:
                k = {kk.strip().lower(): vv.strip() for kk, vv in row.items()}
                rows.append((
                    clean_cnpj(k.get("cnpj") or ""),
                    k.get("nome da empresa") or k.get("nome empresa") or None,
                    parse_date(k.get("data de assinatura") or ""),
                    parse_date(k.get("data de publicação") or k.get("data publicacao") or ""),
                    k.get("órgão responsável") or k.get("orgao") or None,
                    k.get("situação") or k.get("situacao") or None,
                    k.get("descrição") or k.get("descricao") or None,
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.acordos_leniencia
                    (cnpj_14, nome_empresa, data_assinatura, data_publicacao,
                     orgao_acordo, situacao, descricao)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} acordos de leniência inseridos", flush=True)
    return count


def main():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    cur.execute("TRUNCATE dados_rfb.cepim RESTART IDENTITY")
    cur.execute("TRUNCATE dados_rfb.acordos_leniencia RESTART IDENTITY")
    conn.commit()

    total = 0
    for endpoint, (label, tag) in DATASETS.items():
        try:
            url, ym = find_url(endpoint, label)
            print(f"\n=== {label} ({ym}) ===", flush=True)
            if tag == "cepim":
                n = process_cepim(url, cur)
            else:
                n = process_leniencia(url, cur)
            conn.commit()
            total += n
        except Exception as e:
            print(f"  ERRO {label}: {e}", flush=True)
            conn.rollback()

    cur.execute("ANALYZE dados_rfb.cepim")
    cur.execute("ANALYZE dados_rfb.acordos_leniencia")
    conn.commit()
    print(f"\n=== CONCLUÍDO: {total:,} registros ===", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
