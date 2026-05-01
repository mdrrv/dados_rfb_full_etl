"""
ETL: deduplica socios por cpf_cnpj_socio e popula dados_rfb.pessoas com IDs canônicos.

Pré-requisito: tabela dados_rfb.socios já populada.
Pós-condição:
  - dados_rfb.pessoas criada e populada
  - dados_rfb.socios.pessoa_id preenchida com FK para pessoas.id
"""

import os
import re
import uuid
import pathlib
import unicodedata
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, ".env")
if not os.path.isfile(dotenv_path):
    local_env = input("Informe o local do .env: ")
    dotenv_path = os.path.join(local_env, ".env")
load_dotenv(dotenv_path=dotenv_path)

DSN = (
    f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} "
    f"host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} "
    f"password={os.getenv('DB_PASSWORD')}"
)
SCHEMA = os.getenv("DB_SCHEMA", "dados_rfb")

# Namespace fixo — garante que o mesmo cpf_cnpj sempre gera o mesmo UUID
UUID_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", "-", name)
    return name[:60]


def make_slug(uid: uuid.UUID, name: str) -> str:
    return f"{str(uid)[:8]}-{normalize_name(name)}"


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_PESSOAS = f"""
CREATE TABLE IF NOT EXISTS "{SCHEMA}".pessoas (
    id          UUID        PRIMARY KEY,
    cpf_cnpj    TEXT        NOT NULL,
    nome        TEXT        NOT NULL,
    slug        TEXT        NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_pessoas_cpf_cnpj ON "{SCHEMA}".pessoas (cpf_cnpj);
CREATE INDEX IF NOT EXISTS idx_pessoas_slug    ON "{SCHEMA}".pessoas (slug);
"""

DDL_FK = f"""
ALTER TABLE "{SCHEMA}".socios
    ADD COLUMN IF NOT EXISTS pessoa_id UUID REFERENCES "{SCHEMA}".pessoas(id);
CREATE INDEX IF NOT EXISTS idx_socios_pessoa_id ON "{SCHEMA}".socios (pessoa_id);
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    print("Criando tabela pessoas e coluna socios.pessoa_id...")
    cur.execute(DDL_PESSOAS)
    cur.execute(DDL_FK)
    conn.commit()

    print("Limpando tabela pessoas para reprocessamento...")
    cur.execute(f'UPDATE "{SCHEMA}".socios SET pessoa_id = NULL')
    cur.execute(f'TRUNCATE "{SCHEMA}".pessoas')
    conn.commit()

    print("Carregando distinct cpf_cnpj_socio + nome...")
    cur.execute(f"""
        SELECT cpf_cnpj_socio, nome_socio_razao_social
        FROM "{SCHEMA}".socios
        WHERE cpf_cnpj_socio IS NOT NULL AND cpf_cnpj_socio <> ''
        GROUP BY cpf_cnpj_socio, nome_socio_razao_social
    """)
    rows = cur.fetchall()
    print(f"  {len(rows):,} combinações únicas encontradas")

    # Chave de identidade: (cpf_cnpj + nome) — CPF mascarado não é único por pessoa
    # O mesmo padrão ***052458** pode representar centenas de pessoas distintas
    seen: dict[tuple[str, str], tuple[uuid.UUID, str]] = {}
    for cpf_cnpj, nome in rows:
        nome_clean = (nome or "").strip().upper()
        key = (cpf_cnpj, nome_clean)
        if key not in seen:
            uid = uuid.uuid5(UUID_NAMESPACE, f"{cpf_cnpj}|{nome_clean}")
            slug = make_slug(uid, nome_clean or cpf_cnpj)
            seen[key] = (uid, nome_clean, slug)

    print(f"  {len(seen):,} pessoas únicas após deduplicação")

    print("Inserindo em pessoas (UPSERT)...")
    insert_sql = f"""
        INSERT INTO "{SCHEMA}".pessoas (id, cpf_cnpj, nome, slug)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    batch = [(str(uid), cpf, nome, slug) for (cpf, _), (uid, nome, slug) in seen.items()]
    cur.executemany(insert_sql, batch)
    conn.commit()
    print(f"  {cur.rowcount:,} linhas inseridas")

    print("Atualizando socios.pessoa_id...")
    cur.execute(f"""
        UPDATE "{SCHEMA}".socios s
        SET pessoa_id = p.id
        FROM "{SCHEMA}".pessoas p
        WHERE s.cpf_cnpj_socio = p.cpf_cnpj
          AND UPPER(TRIM(s.nome_socio_razao_social)) = p.nome
          AND s.pessoa_id IS NULL
    """)
    conn.commit()
    print(f"  {cur.rowcount:,} vínculos atualizados")

    cur.close()
    conn.close()
    print("Concluído.")


if __name__ == "__main__":
    main()
