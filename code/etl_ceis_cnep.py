"""
ETL CEIS/CNEP — Portal da Transparência (CGU)
Baixa, extrai e importa sanções federais para dados_rfb.sancoes_federais.
"""
import os, io, csv, zipfile, pathlib, requests, psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(dotenv_path=str(pathlib.Path().resolve() / ".env"))

DB_HOST = os.getenv("DB_HOST", "187.127.13.118")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")
DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados"
DATASETS = {"ceis": "CEIS", "cnep": "CNEP"}


def find_latest_url(dataset: str, label: str) -> tuple[str, str]:
    today = datetime.today()
    for delta in range(0, 6):
        dt = today - timedelta(days=delta * 30)
        ym = dt.strftime("%Y%m")
        url = f"{BASE_URL}/{dataset}/{ym}_{label}.zip"
        r = requests.head(url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return url, ym
    raise RuntimeError(f"Nenhum arquivo {label} encontrado nos últimos 6 meses")


DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.sancoes_federais (
    id                  SERIAL PRIMARY KEY,
    fonte               VARCHAR(4)   NOT NULL,          -- CEIS | CNEP
    cnpj_cpf            VARCHAR(18),
    cnpj_14             VARCHAR(14),
    nome_sancionado     TEXT,
    tipo_sancao         TEXT,
    data_inicio         DATE,
    data_fim            DATE,
    orgao_sancionador   TEXT,
    uf_orgao            VARCHAR(2),
    processo            TEXT,
    ativo               BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_sancoes_cnpj14 ON dados_rfb.sancoes_federais(cnpj_14);
CREATE INDEX IF NOT EXISTS idx_sancoes_ativo  ON dados_rfb.sancoes_federais(ativo) WHERE ativo;
CREATE INDEX IF NOT EXISTS idx_sancoes_fonte  ON dados_rfb.sancoes_federais(fonte);
"""


def parse_date(s: str) -> "str | None":
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def clean_cnpj(raw: str) -> "str | None":
    digits = "".join(c for c in (raw or "") if c.isdigit())
    return digits[:14] if len(digits) >= 14 else None


def process_zip(fonte: str, url: str, cur) -> int:
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
            for row in reader:
                keys = {k.strip().lower(): v.strip() for k, v in row.items()}
                cnpj_raw = (
                    keys.get("cadastro cpf ou cnpj do sancionado")
                    or keys.get("cadastro cnpj do sancionado")
                    or keys.get("cnpj")
                    or ""
                )
                data_ini = parse_date(
                    keys.get("data de início da sanção")
                    or keys.get("data inicio da sancao")
                    or keys.get("datainiiciosancao", "")
                )
                data_fim_raw = (
                    keys.get("data de fim da sanção")
                    or keys.get("data fim da sancao")
                    or keys.get("datafimdasancao", "")
                )
                data_fim = parse_date(data_fim_raw) if data_fim_raw else None

                ativo = True
                if data_fim:
                    ativo = datetime.strptime(data_fim, "%Y-%m-%d") >= datetime.today()

                rows.append((
                    fonte,
                    cnpj_raw or None,
                    clean_cnpj(cnpj_raw),
                    keys.get("nome informado pelo órgão sancionador") or keys.get("nome do sancionado") or None,
                    keys.get("tipo de sanção") or keys.get("tipo de sancao") or None,
                    data_ini,
                    data_fim,
                    keys.get("órgão sancionador") or keys.get("orgao sancionador") or None,
                    (keys.get("uf do órgão sancionador") or keys.get("uf orgao") or "")[:2] or None,
                    keys.get("número do processo") or keys.get("numero do processo") or None,
                    ativo,
                ))

            cur.executemany("""
                INSERT INTO dados_rfb.sancoes_federais
                    (fonte, cnpj_cpf, cnpj_14, nome_sancionado, tipo_sancao,
                     data_inicio, data_fim, orgao_sancionador, uf_orgao, processo, ativo)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} registros inseridos de {name}", flush=True)
    return count


def main():
    import sys
    # Usage: python etl_ceis_cnep.py [ceis.zip] [cnep.zip]
    # If no args, tries to download from Portal da Transparência.
    # Portal da Transparência may require manual download (WAF/captcha protection).
    local_files = {k: v for k, v in zip(["ceis", "cnep"], sys.argv[1:])}

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: sancoes_federais ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    print("=== Truncando tabela ===", flush=True)
    cur.execute("TRUNCATE dados_rfb.sancoes_federais RESTART IDENTITY")
    conn.commit()

    total = 0
    for dataset, label in DATASETS.items():
        print(f"\n=== {label} ===", flush=True)
        try:
            if dataset in local_files:
                path = local_files[dataset]
                print(f"  Usando arquivo local: {path}", flush=True)
                with open(path, "rb") as f:
                    content = io.BytesIO(f.read())
                count = 0
                with zipfile.ZipFile(content) as zf:
                    for name in zf.namelist():
                        if not name.upper().endswith(".CSV"):
                            continue
                        with zf.open(name) as f2:
                            text = f2.read().decode("latin-1", errors="replace")
                        import csv as csv_mod
                        reader = csv_mod.DictReader(io.StringIO(text), delimiter=";")
                        rows = []
                        for row in reader:
                            keys = {k.strip().lower(): v.strip() for k, v in row.items()}
                            cnpj_raw = (keys.get("cadastro cpf ou cnpj do sancionado") or keys.get("cnpj") or "")
                            data_ini = parse_date(keys.get("data de início da sanção") or "")
                            data_fim_raw = keys.get("data de fim da sanção") or ""
                            data_fim = parse_date(data_fim_raw) if data_fim_raw else None
                            ativo = not bool(data_fim) or (datetime.strptime(data_fim, "%Y-%m-%d") >= datetime.today() if data_fim else True)
                            rows.append((label, cnpj_raw or None, clean_cnpj(cnpj_raw), keys.get("nome informado pelo órgão sancionador") or None, keys.get("tipo de sanção") or None, data_ini, data_fim, keys.get("órgão sancionador") or None, (keys.get("uf do órgão sancionador") or "")[:2] or None, keys.get("número do processo") or None, ativo,))
                        cur.executemany("INSERT INTO dados_rfb.sancoes_federais (fonte,cnpj_cpf,cnpj_14,nome_sancionado,tipo_sancao,data_inicio,data_fim,orgao_sancionador,uf_orgao,processo,ativo) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
                        count += len(rows)
                        print(f"  {len(rows):,} registros de {name}", flush=True)
                conn.commit()
                total += count
            else:
                url, ym = find_latest_url(dataset, label)
                print(f"  Arquivo encontrado: {ym}", flush=True)
                n = process_zip(label, url, cur)
                conn.commit()
                total += n
        except Exception as e:
            print(f"  ERRO {label}: {e}", flush=True)
            conn.rollback()

    print(f"\n=== ANALYZE ===", flush=True)
    cur.execute("ANALYZE dados_rfb.sancoes_federais")
    conn.commit()

    print(f"\n=== CONCLUÍDO: {total:,} sanções importadas ===", flush=True)
    print("\nNOTA: Se download falhou, baixe manualmente em:")
    print("  CEIS: https://portaldatransparencia.gov.br/download-de-dados/ceis")
    print("  CNEP: https://portaldatransparencia.gov.br/download-de-dados/cnep")
    print("  Uso: python etl_ceis_cnep.py /caminho/CEIS.zip /caminho/CNEP.zip")
    conn.close()


if __name__ == "__main__":
    main()
