# Task 4 - Индексатор (C++ без STL)

## Сборка

```sh
cmake -S . -B build
cmake --build build
```

Бинарник: `build/ir_indexer`.

## Запуск

```sh
./build/ir_indexer --mongo-uri mongodb://admin:password@mongo:27017/?authSource=admin \
  --db infosearch --collection articles --out output --limit 0
```

Выходные файлы:
- `output/docs.tsv`
- `output/index.tsv`
- `output/freq.csv`
- `output/stats.txt`

## Docker Compose

```sh
docker compose build indexer
docker compose run --rm indexer
```

Переменные окружения:
- `MONGO_URI`
- `DB_NAME`
- `COLLECTION`
- `OUT_DIR`
- `LIMIT`
