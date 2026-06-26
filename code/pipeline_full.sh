#!/bin/bash
# Pipeline ETL completo — ordem obrigatória
# Uso: docker run --rm cnpj_etl_worker bash code/pipeline_full.sh
set -e

echo "=== [1/6] ETL Postgres (download + carga raw) ===" && python code/etl_postgres.py
echo "=== [2/6] Consolidar cnpj_consolidado ===" && python code/consolidar_fast.py
echo "=== [3/6] Build socios_consolidado ===" && python code/build_socios_consolidado.py
echo "=== [4/6] Build pessoas_consolidado ===" && python code/build_pessoas_consolidado.py
echo "=== [5/6] Meilisearch: indexar pessoas ===" && python code/build_meili_socios.py
echo "=== [6/6] Meilisearch: indexar socios ===" && python code/build_meili_socios_idx.py

echo "=== Cleanup: liberando ZIPs e CSVs (ja importados no Postgres) ==="
# Bypass com KEEP_ETL_FILES=1 caso queira inspecionar
if [ "${KEEP_ETL_FILES:-0}" = "1" ]; then
  echo "KEEP_ETL_FILES=1 -- mantendo arquivos."
else
  : "${EXTRACTED_FILES_PATH:?EXTRACTED_FILES_PATH nao definido}"
  : "${OUTPUT_FILES_PATH:?OUTPUT_FILES_PATH nao definido}"
  # Tamanho liberado pra log
  freed=$(du -shc "$EXTRACTED_FILES_PATH" "$OUTPUT_FILES_PATH" 2>/dev/null | tail -1 | awk '{print $1}')
  rm -rf "$EXTRACTED_FILES_PATH"/* "$OUTPUT_FILES_PATH"/*
  echo "Liberado: ${freed:-?}"
fi

echo "=== Pipeline concluido ==="
