# Task 3 - Токенизация и закон Ципфа

## Сборка

```sh
cmake -S . -B build
cmake --build build
```

Бинарник: `build/tokenize` (Windows: `build\Debug\tokenize.exe` или `build\Release\tokenize.exe`).

## Запуск

Один файл:

```sh
build/tokenize --input path/to/file.txt --out freq.csv
```

Несколько файлов (файл-список: один путь на строку):

```sh
build/tokenize --list filelist.txt --out freq.csv
```

Создание файла-списка (PowerShell):

```sh
Get-ChildItem -Recurse -File path\to\corpus | ForEach-Object { $_.FullName } | Set-Content filelist.txt
```

Опциональные флаги:
- `--strip-html` - игнорировать текст внутри тегов `<...>`.
- `--fit-mandelbrot` - подогнать константы Мандельброта и добавить кривую в CSV.

## Docker Compose

Сборка и запуск токенизации в контейнере:

```sh
docker compose build tokenize
docker compose run --rm tokenize
```

Выход будет записан в `task_3/output/freq.csv`.

Переменные окружения:
- `STRIP_HTML` (по умолчанию `1`)
- `FIT_MANDELBROT` (по умолчанию `0`)
- `OUT_CSV` (по умолчанию `/out/freq.csv`)
- `TOKENIZE_ARGS` (дополнительные аргументы CLI)

## Поля вывода

В консоли:
- `Bytes`
- `Tokens`
- `AvgTokenLen`
- `UniqueTerms`
- `TimeSeconds`
- `SpeedKBps`
- `Mandelbrot` (если включено)

## График

```sh
python plot.py --input freq.csv --out zipf.png
```

График строится в лог-лог шкале и накладывает эмпирическое распределение на Zipf и (опционально) кривую Мандельброта.
