#!/usr/bin/env bash
# Converte example/sample.json para example/sample.parquet usando o conversor do repositório.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/.." && pwd)"

cd "$ROOT"

IN="$DIR/sample.json"
OUT="$DIR/sample.parquet"

go run ./cmd/json2parquet -i "$IN" -o "$OUT" -compression snappy

echo "Conversão concluída: $OUT"
