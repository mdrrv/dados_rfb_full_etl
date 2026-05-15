"""
ETL PGFN — Dívida Ativa Federal (Procuradoria-Geral da Fazenda Nacional)
Fonte: https://www.pgfn.gov.br/acesso-a-informacao/dados-abertos
CSV trimestral público — sem WAF/captcha.

URLs base:
  https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN/{ANO}/{TRIMESTRE}.zip
  Ex: https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN/2024/FGTS_202401.zip

Colunas esperadas no CSV:
  CPF_CNPJ, TIPO_PESSOA, TIPO_DEVEDOR, NOME_DEVEDOR,
  VALOR_CONSOLIDADO, SITUACAO_INSCRICAO, TIPO_SITUACAO_ATIVA,
  DATA_INSCRICAO, NUMERO_INSCRICAO, INDICADOR_AJUIZADO

Uso:
  python etl_pgfn.py                    # baixa automaticamente o trimestre mais recente
  python etl_pgfn.py /path/PGFN.zip     # arquivo local
"""
import os, io, csv, zipfile, pathlib, requests, psycopg2, re, sys
from datetime import date
from dotenv import load_dotenv

load_dotenv(dotenv_path=str(pathlib.Path().resolve() / ".env"))

DB_HOST = os.getenv("DB_HOST", "187.127.13.118")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")
DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

BASE_URL = "https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN"
# Arquivos disponíveis por tipo de dívida (RURAL removido — não existe no PGFN aberto)
TIPOS = ["FGTS", "NAO_PREVIDENCIARIO", "PREVIDENCIARIO"]

DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.pgfn_divida_ativa (
    id                  SERIAL PRIMARY KEY,
    cpf_cnpj            VARCHAR(18),
    cnpj_14             VARCHAR(14),
    tipo_pessoa         VARCHAR(1),
    tipo_devedor        TEXT,
    nome_devedor        TEXT,
    uf_devedor          VARCHAR(2),
    tipo_divida         VARCHAR(32),
    valor_consolidado   NUMERIC(18,2),
    situacao_inscricao  TEXT,
    tipo_situacao_ativa TEXT,
    data_inscricao      DATE,
    numero_inscricao    TEXT,
    indicador_ajuizado  VARCHAR(1)
);

CREATE INDEX IF NOT EXISTS idx_pgfn_cnpj14  ON dados_rfb.pgfn_divida_ativa(cnpj_14);
CREATE INDEX IF NOT EXISTS idx_pgfn_cpfcnpj ON dados_rfb.pgfn_divida_ativa(cpf_cnpj);
"""


def clean_cnpj(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 14:
        return digits
    return None


def parse_date(s: str) -> "date | None":
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return date(*[int(x) for x in __import__("datetime").datetime.strptime(s, fmt).timetuple()[:3]])
        except Exception:
            pass
    return None


def parse_valor(s: str) -> "float | None":
    s = (s or "").strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def find_latest_url(tipo: str) -> "str | None":
    today = date.today()
    for year in range(today.year, today.year - 3, -1):
        for month in range(12, 0, -1):
            # Skip future months
            if year == today.year and month > today.month:
                continue
            yymm = f"{year}{month:02d}"
            candidates = [
                f"{BASE_URL}/{year}/{tipo}_{yymm}.zip",
                f"{BASE_URL}/{tipo}_{yymm}.zip",
            ]
            for url in candidates:
                try:
                    r = requests.head(url, timeout=8, allow_redirects=True)
                    if r.status_code == 200:
                        return url
                except Exception:
                    pass
    return None


def process_file(buf: io.BytesIO, tipo: str, cur) -> int:
    count = 0
    with zipfile.ZipFile(buf) as zf:
        names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        if not names:
            print(f"  Nenhum CSV em {tipo}", flush=True)
            return 0
        for name in names:
            print(f"  Processando {name} ...", flush=True)
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            for row in reader:
                k = {kk.strip(): vv.strip() for kk, vv in row.items()}
                cpf_cnpj = (k.get("CPF_CNPJ") or k.get("CNPJ_CPF") or "").strip()
                cnpj14 = clean_cnpj(cpf_cnpj)
                rows.append((
                    cpf_cnpj or None,
                    cnpj14,
                    (k.get("TIPO_PESSOA") or "")[:1] or None,
                    k.get("TIPO_DEVEDOR") or None,
                    (k.get("NOME_DEVEDOR") or "").upper().strip() or None,
                    (k.get("UF_DEVEDOR") or "")[:2] or None,
                    tipo,
                    parse_valor(k.get("VALOR_CONSOLIDADO") or ""),
                    k.get("SITUACAO_INSCRICAO") or None,
                    k.get("TIPO_SITUACAO_ATIVA") or None,
                    parse_date(k.get("DATA_INSCRICAO") or ""),
                    k.get("NUMERO_INSCRICAO") or None,
                    (k.get("INDICADOR_AJUIZADO") or "")[:1] or None,
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.pgfn_divida_ativa
                    (cpf_cnpj, cnpj_14, tipo_pessoa, tipo_devedor, nome_devedor, uf_devedor,
                     tipo_divida, valor_consolidado, situacao_inscricao, tipo_situacao_ativa,
                     data_inscricao, numero_inscricao, indicador_ajuizado)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} registros de {name}", flush=True)
    return count


def main():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: pgfn_divida_ativa ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    print("=== Truncando tabela ===", flush=True)
    cur.execute("TRUNCATE dados_rfb.pgfn_divida_ativa RESTART IDENTITY")
    conn.commit()

    local_files = sys.argv[1:]
    total = 0

    if local_files:
        for path in local_files:
            tipo = pathlib.Path(path).stem.split("_")[0].upper()
            print(f"\n=== Arquivo local: {path} (tipo={tipo}) ===", flush=True)
            try:
                with open(path, "rb") as f:
                    buf = io.BytesIO(f.read())
                n = process_file(buf, tipo, cur)
                conn.commit()
                total += n
            except Exception as e:
                print(f"  ERRO: {e}", flush=True)
                conn.rollback()
    else:
        for tipo in TIPOS:
            url = find_latest_url(tipo)
            if not url:
                print(f"\n=== {tipo}: nenhuma URL encontrada, pulando ===", flush=True)
                continue
            print(f"\n=== {tipo}: {url} ===", flush=True)
            try:
                r = requests.get(url, timeout=300, stream=True)
                r.raise_for_status()
                n = process_file(io.BytesIO(r.content), tipo, cur)
                conn.commit()
                total += n
            except Exception as e:
                print(f"  ERRO {tipo}: {e}", flush=True)
                conn.rollback()

    print("\n=== ANALYZE ===", flush=True)
    cur.execute("ANALYZE dados_rfb.pgfn_divida_ativa")
    conn.commit()
    print(f"\n=== CONCLUIDO: {total:,} registros ===", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
