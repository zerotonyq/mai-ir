# Task 5 — Поиск (CLI + WEB)

## Сборка

```sh
cmake -S . -B build
cmake --build build
```

Бинарники:
- `build/ir_cli`
- `build/ir_web`

## CLI

```sh
./build/ir_cli --index ../task_4/output/index.tsv --docs ../task_4/output/docs.tsv --query "(linux or windows) and not python"
```

## WEB

```sh
./build/ir_web --index ../task_4/output/index.tsv --docs ../task_4/output/docs.tsv --port 8080
```

Открыть `http://localhost:8080`.

## Docker Compose

```sh
docker compose build web cli
# сначала собрать индекс
docker compose --profile tools run --rm indexer
# запустить web
docker compose up web
```

CLI запускается через `docker compose run --rm cli --query "..."`.
