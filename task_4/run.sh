#!/bin/sh
set -eu

MONGO_URI=${MONGO_URI:-mongodb://admin:password@mongo:27017/?authSource=admin}
DB_NAME=${DB_NAME:-crawler_db}
COLLECTION=${COLLECTION:-articles}
OUT_DIR=${OUT_DIR:-/out}
LIMIT=${LIMIT:-0}

mkdir -p "$OUT_DIR"

args="--mongo-uri $MONGO_URI --db $DB_NAME --collection $COLLECTION --out $OUT_DIR"
if [ "$LIMIT" != "0" ]; then
  args="$args --limit $LIMIT"
fi

/app/build/ir_indexer $args

echo "Done. Output in $OUT_DIR"
