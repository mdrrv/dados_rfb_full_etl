"""
ETL Despesas por Favorecido (Portal da Transparência CGU)
Fonte: https://portaldatransparencia.gov.br/download-de-dados/despesas

BLOQUEIO (2026): Portal usa CloudFront + AWS WAF captcha — download manual necessário.
Uso: python etl_despesas_favorecido.py /path/despesas_YYYYMM.zip [...]

Colunas esperadas:
  ANO_EXTRATO, MES_EXTRATO, CODIGO_ORGAO_SUPERIOR, NOME_ORGAO_SUPERIOR,
  CODIGO_ORGAO, NOME_ORGAO, CODIGO_UNIDADE_GESTORA, NOME_UNIDADE_GESTORA,
  CODIGO_FAVORECIDO, NOME_FAVORECIDO, TIPO_FAVORECIDO,
  DOCUMENTO_FAVORECIDO, VALOR_BRUTO, VALOR_DESCONTO, VALOR_LIQUIDO
"""
import os, io, csv, zipfile, pathlib, psycopg2, re, sys
from dotenv import load_dotenv

load_dotenv(dotenv_path=str(pathlib.Path().resolve() / ".env"))

DB_HOST = os.getenv("DB_HOST", "187.127.13.118")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")
DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.despesas_favorecido (
    id                      SERIAL PRIMARY KEY,
    ano_extrato             SMALLINT,
    mes_extrato             SMALLINT,
    cnpj_14                 VARCHAR(14),
    cpf                     VARCHAR(11),
    documento_favorecido    TEXT,
    nome_favorecido         TEXT,
    tipo_favorecido         TEXT,
    nome_orgao              TEXT,
    valor_bruto             NUMERIC(18,2),
    valor_liquido           NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS idx_desp_cnpj14 ON dados_rfb.despesas_favorecido(cnpj_14);
CREATE INDEX IF NOT EXISTS idx_desp_cpf    ON dados_rfb.despesas_favorecido(cpf);
CREATE INDEX IF NOT EXISTS idx_desp_ano    ON dados_rfb.despesas_favorecido(ano_extrato);
"""


def clean_cnpj(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits if len(digits) == 14 else None


def clean_cpf(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits[:11] if len(digits) == 11 else None


def parse_valor(s: str) -> "float | None":
    s = (s or "").strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def process_zip(path_or_buf, cur) -> int:
    count = 0
    if isinstance(path_or_buf, (str, pathlib.Path)):
        with open(path_or_buf, "rb") as f:
            buf = io.BytesIO(f.read())
    else:
        buf = path_or_buf
    with zipfile.ZipFile(buf) as zf:
        names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        for name in names:
            print(f"  Processando {name} ...", flush=True)
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            for row in reader:
                k = {kk.strip().upper(): vv.strip() for kk, vv in row.items()}
                doc = (k.get("DOCUMENTO_FAVORECIDO") or k.get("CPF_CNPJ_FAVORECIDO") or "").strip()
                cnpj14 = clean_cnpj(doc)
                cpf = clean_cpf(doc) if not cnpj14 else None
                nome = (k.get("NOME_FAVORECIDO") or "").upper().strip() or None
                try:
                    ano = int(k.get("ANO_EXTRATO") or 0) or None
                    mes = int(k.get("MES_EXTRATO") or 0) or None
                except Exception:
                    ano = mes = None
                rows.append((
                    ano,
                    mes,
                    cnpj14,
                    cpf,
                    doc or None,
                    nome,
                    k.get("TIPO_FAVORECIDO") or None,
                    k.get("NOME_ORGAO") or k.get("NOME_ORGAO_SUPERIOR") or None,
                    parse_valor(k.get("VALOR_BRUTO") or ""),
                    parse_valor(k.get("VALOR_LIQUIDO") or ""),
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.despesas_favorecido
                    (ano_extrato, mes_extrato, cnpj_14, cpf, documento_favorecido,
                     nome_favorecido, tipo_favorecido, nome_orgao, valor_bruto, valor_liquido)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} registros de {name}", flush=True)
    return count


def main():
    if not sys.argv[1:]:
        print("BLOQUEIO: Portal da Transparência usa WAF/captcha em 2026.", flush=True)
        print("Baixe manualmente: https://portaldatransparencia.gov.br/download-de-dados/despesas", flush=True)
        print("Uso: python etl_despesas_favorecido.py /path/despesas_202501.zip [...]", flush=True)
        return

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: despesas_favorecido ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    total = 0
    for path in sys.argv[1:]:
        print(f"\n=== Arquivo: {path} ===", flush=True)
        try:
            n = process_zip(path, cur)
            conn.commit()
            total += n
        except Exception as e:
            print(f"  ERRO: {e}", flush=True)
            conn.rollback()

    cur.execute("ANALYZE dados_rfb.despesas_favorecido")
    conn.commit()
    print(f"\n=== CONCLUIDO: {total:,} registros ===", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
