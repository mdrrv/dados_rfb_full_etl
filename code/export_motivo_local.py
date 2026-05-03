"""
Passo 1/2 — roda LOCAL.
Lê os arquivos ESTABELE* e gera motivo_cnpj.csv.gz com apenas
(cnpj, motivo_situacao_cadastral). Arquivo de saída fica em OUTPUT_FILES_PATH.

Uso:
    cd dados_rfb_full_etl
    python code/export_motivo_local.py
    # Depois: scp <output>/motivo_cnpj.csv.gz root@187.127.13.118:/tmp/
"""
import gzip
import os
import pathlib
import time

import polars as pl
from dotenv import load_dotenv


def _find_dotenv() -> str:
    p = pathlib.Path().resolve()
    for _ in range(5):
        candidate = p / ".env"
        if candidate.is_file():
            return str(candidate)
        p = p.parent
    while True:
        raw = input("Caminho do .env: ").strip().strip("'\"")
        if raw.startswith("#") or not raw:
            continue
        p = pathlib.Path(raw)
        path = p if p.name == ".env" else p / ".env"
        if path.is_file():
            return str(path)
        print(f"  Não encontrado: {path}")


load_dotenv(_find_dotenv(), override=True)

extracted_files = os.getenv("EXTRACTED_FILES_PATH")
output_path     = os.getenv("OUTPUT_FILES_PATH", ".")

if not extracted_files or not os.path.isdir(extracted_files):
    raise SystemExit(f"ERRO: EXTRACTED_FILES_PATH inválido: {extracted_files}")

ALL_ESTAB_COLS = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
    'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
    'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
    'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
    'tipo_logradouro', 'logradouro', 'numero', 'complemento',
    'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
    'ddd_2', 'telefone_2', 'correio_eletronico',
    'situacao_especial', 'data_situacao_especial',
]

arquivos = sorted([f for f in os.listdir(extracted_files) if "ESTABELE" in f.upper()])
if not arquivos:
    raise SystemExit(f"ERRO: Nenhum arquivo ESTABELE* em: {extracted_files}")

print(f"{len(arquivos)} arquivo(s) encontrado(s). Gerando motivo_cnpj.csv.gz...")

out_file = os.path.join(output_path, "motivo_cnpj.csv.gz")
start = time.time()
total = 0

with gzip.open(out_file, "wt", encoding="utf-8") as gz:
    gz.write("cnpj,motivo_situacao_cadastral\n")
    for arquivo in arquivos:
        t0 = time.time()
        print(f"  {arquivo}... ", end="", flush=True)

        df = pl.read_csv(
            os.path.join(extracted_files, arquivo),
            separator=";",
            has_header=False,
            encoding="latin1",
            infer_schema_length=0,
            new_columns=ALL_ESTAB_COLS,
        ).select([
            (pl.col("cnpj_basico") + pl.col("cnpj_ordem") + pl.col("cnpj_dv")).alias("cnpj"),
            pl.col("motivo_situacao_cadastral"),
        ])

        gz.write(df.write_csv(include_header=False))
        total += len(df)
        print(f"{len(df):,} linhas ({round(time.time()-t0)}s)", flush=True)

size_mb = os.path.getsize(out_file) / 1024 / 1024
print(f"\nArquivo gerado: {out_file}")
print(f"Tamanho: {size_mb:.1f} MB | {total:,} registros | {round(time.time()-start)}s")
print(f"\nPróximo passo:")
print(f"  scp \"{out_file}\" root@187.127.13.118:/tmp/motivo_cnpj.csv.gz")
