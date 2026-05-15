"""
ETL Servidores Públicos Federais (Portal da Transparência CGU)
Fonte: https://portaldatransparencia.gov.br/download-de-dados/servidores

BLOQUEIO (2026): Portal usa CloudFront + AWS WAF captcha — download manual necessário.
Uso: python etl_servidores.py /path/servidores_YYYYMM.zip [...]

Colunas esperadas (BACEN/Civil):
  Id_SERVIDOR_PORTAL, NOME, CPF, MATRICULA, DESCRICAO_CARGO,
  UORG_LOTACAO, ORG_LOTACAO, SITUACAO_VINCULO, REGIME_JURIDICO,
  JORNADA_DE_TRABALHO, DATA_INGRESSO_CARGOFUNCAO, DATA_INGRESSO_ORGAO,
  DOCUMENTO_INGRESSO_SERVICO, DATA_DIPLOMA_INGRESSO_SERVICO
"""
import os, io, csv, zipfile, pathlib, psycopg2, re, sys
from datetime import date
from dotenv import load_dotenv

load_dotenv(dotenv_path=str(pathlib.Path().resolve() / ".env"))

DB_HOST = os.getenv("DB_HOST", "187.127.13.118")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")
DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", 5432))

DDL = """
CREATE TABLE IF NOT EXISTS dados_rfb.servidores_federais (
    id                  SERIAL PRIMARY KEY,
    cpf                 VARCHAR(14) INDEX,
    nome                TEXT,
    matricula           TEXT,
    descricao_cargo     TEXT,
    uorg_lotacao        TEXT,
    org_lotacao         TEXT,
    situacao_vinculo    TEXT,
    regime_juridico     TEXT,
    jornada             TEXT,
    data_ingresso_cargo DATE,
    data_ingresso_orgao DATE
);

CREATE INDEX IF NOT EXISTS idx_serv_cpf  ON dados_rfb.servidores_federais(cpf);
CREATE INDEX IF NOT EXISTS idx_serv_nome ON dados_rfb.servidores_federais(nome);
"""

FIELD_MAP = {
    "cpf":                 ["CPF"],
    "nome":                ["NOME"],
    "matricula":           ["MATRICULA"],
    "descricao_cargo":     ["DESCRICAO_CARGO", "CARGO"],
    "uorg_lotacao":        ["UORG_LOTACAO"],
    "org_lotacao":         ["ORG_LOTACAO"],
    "situacao_vinculo":    ["SITUACAO_VINCULO"],
    "regime_juridico":     ["REGIME_JURIDICO"],
    "jornada":             ["JORNADA_DE_TRABALHO"],
    "data_ingresso_cargo": ["DATA_INGRESSO_CARGOFUNCAO"],
    "data_ingresso_orgao": ["DATA_INGRESSO_ORGAO"],
}


def clean_cpf(raw: str) -> "str | None":
    digits = re.sub(r"\D", "", raw or "")
    return digits[:11] if len(digits) >= 11 else None


def parse_date(s: str) -> "date | None":
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            import datetime
            return datetime.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def get_field(k: dict, candidates: list[str]) -> "str | None":
    for c in candidates:
        v = k.get(c)
        if v is not None:
            return v.strip() or None
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
        # Process only the cadastro files, skip "remuneracao"
        targets = [n for n in names if "REMUN" not in n.upper()] or names
        for name in targets:
            print(f"  Processando {name} ...", flush=True)
            with zf.open(name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = []
            for row in reader:
                k = {kk.strip().upper(): vv.strip() for kk, vv in row.items()}
                nome = get_field(k, FIELD_MAP["nome"])
                if not nome:
                    continue
                rows.append((
                    clean_cpf(get_field(k, FIELD_MAP["cpf"]) or ""),
                    nome.upper(),
                    get_field(k, FIELD_MAP["matricula"]),
                    get_field(k, FIELD_MAP["descricao_cargo"]),
                    get_field(k, FIELD_MAP["uorg_lotacao"]),
                    get_field(k, FIELD_MAP["org_lotacao"]),
                    get_field(k, FIELD_MAP["situacao_vinculo"]),
                    get_field(k, FIELD_MAP["regime_juridico"]),
                    get_field(k, FIELD_MAP["jornada"]),
                    parse_date(get_field(k, FIELD_MAP["data_ingresso_cargo"]) or ""),
                    parse_date(get_field(k, FIELD_MAP["data_ingresso_orgao"]) or ""),
                ))
            cur.executemany("""
                INSERT INTO dados_rfb.servidores_federais
                    (cpf, nome, matricula, descricao_cargo, uorg_lotacao, org_lotacao,
                     situacao_vinculo, regime_juridico, jornada, data_ingresso_cargo, data_ingresso_orgao)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            count += len(rows)
            print(f"  {len(rows):,} servidores de {name}", flush=True)
    return count


def main():
    if not sys.argv[1:]:
        print("BLOQUEIO: Portal da Transparência usa WAF/captcha em 2026.", flush=True)
        print("Baixe manualmente: https://portaldatransparencia.gov.br/download-de-dados/servidores", flush=True)
        print("Uso: python etl_servidores.py /path/servidores_202501.zip [...]", flush=True)
        return

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("=== DDL: servidores_federais ===", flush=True)
    cur.execute(DDL)
    conn.commit()

    print("=== Truncando tabela ===", flush=True)
    cur.execute("TRUNCATE dados_rfb.servidores_federais RESTART IDENTITY")
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

    cur.execute("ANALYZE dados_rfb.servidores_federais")
    conn.commit()
    print(f"\n=== CONCLUIDO: {total:,} servidores ===", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
