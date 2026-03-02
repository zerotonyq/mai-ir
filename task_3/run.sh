#!/bin/sh
set -eu

DATA_DIR=${DATA_DIR:-/data}
OUT_DIR=${OUT_DIR:-/out}
OUT_CSV=${OUT_CSV:-/out/freq.csv}
STRIP_HTML=${STRIP_HTML:-1}
FIT_MANDELBROT=${FIT_MANDELBROT:-0}
TOKENIZE_ARGS=${TOKENIZE_ARGS:-}

list=/tmp/filelist.txt
mkdir -p "$OUT_DIR"
find "$DATA_DIR" -type f > "$list"

args=""
if [ "$STRIP_HTML" = "1" ]; then args="$args --strip-html"; fi
if [ "$FIT_MANDELBROT" = "1" ]; then args="$args --fit-mandelbrot"; fi

/app/tokenize --list "$list" --out "$OUT_CSV" $args $TOKENIZE_ARGS

echo "Done. Output: $OUT_CSV"
