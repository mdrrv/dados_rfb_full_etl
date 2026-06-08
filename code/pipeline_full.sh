#!/bin/bash
# Pipeline ETL completo — ordem obrigatória
# Uso: docker run --rm cnpj_etl_worker bash code/pipeline_full.sh
set -e

echo "=== [1/5] ETL Postgres (download + carga raw) ===" && python code/etl_postgres.py
echo "=== [2/5] Consolidar cnpj_consolidado ===" && python code/consolidar_fast.py
echo "=== [3/5] Build socios_consolidado ===" && python code/build_socios_consolidado.py
echo "=== [4/5] Build pessoas_consolidado ===" && python code/build_pessoas_consolidado.py
echo "=== [5/5] Meilisearch: indexar pessoas ===" && python code/build_meili_socios.py
echo "=== Pipeline concluido ==="
