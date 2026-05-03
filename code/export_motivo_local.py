"""
Passo 1/2 — roda LOCAL.
Lê os arquivos ESTABELE* e gera motivo_cnpj.csv.gz com apenas
(cnpj, motivo_situacao_cadastral). Arquivo de saída fica em OUTPUT_FILES_PATH.

Uso:
    cd dados_rfb_full_etl
    python code/export_motivo_local.py
    # Depois: scp <output>/motivo_cnpj.csv.gz root@187.127.13.118:/tmp/
"""
import csv
import gzip
import os
import pathlib
import time

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

# Leitura linha a linha — sem carregar o arquivo inteiro na memória
# pos 0: cnpj_basico | 1: cnpj_ordem | 2: cnpj_dv | 7: motivo_situacao_cadastral

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
        count = 0

        with open(os.path.join(extracted_files, arquivo), "rb") as raw:
            reader = csv.reader(
                (line.decode("latin1") for line in raw),
                delimiter=";",
            )
            for row in reader:
                if len(row) < 8:
                    continue
                cnpj   = row[0] + row[1] + row[2]
                motivo = row[7]
                gz.write(f"{cnpj},{motivo}\n")
                count += 1

        total += count
        print(f"{count:,} linhas ({round(time.time()-t0)}s)", flush=True)

size_mb = os.path.getsize(out_file) / 1024 / 1024
print(f"\nArquivo gerado: {out_file}")
print(f"Tamanho: {size_mb:.1f} MB | {total:,} registros | {round(time.time()-start)}s")
print(f"\nPróximo passo:")
print(f"  scp \"{out_file}\" root@187.127.13.118:/tmp/motivo_cnpj.csv.gz")
