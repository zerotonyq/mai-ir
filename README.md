# InfoSearch (MAI)

Учебный проект по информационному поиску: сбор корпуса, хранение в MongoDB, построение булевого инвертированного индекса и выполнение булевых запросов через CLI и WEB.

## Структура репозитория

- `task_1` - скрапинг статей, подготовка JSONL и загрузка в MongoDB.
- `task_2` - краулер (C, libcurl + MongoDB C driver).
- `task_3` - токенизация и закон Ципфа (C + Python график).
- `task_4` - индексатор (C++ без STL, вход из MongoDB).
- `task_5` - поиск (CLI + WEB-интерфейс, C++ без STL).
- `task_6` - LaTeX отчёт и графики.

## Быстрый старт (Docker)

1. Поднять MongoDB:
```sh
docker compose up -d mongo
```

2. Импортировать корпус (см. раздел ниже) или подготовить коллекцию `articles` другим способом.

3. Построить индекс:
```sh
docker compose --profile tools run --rm indexer
```

4. Запустить WEB-интерфейс:
```sh
docker compose up web
```

Открыть `http://localhost:8080`.

## CLI поиск

```sh
docker compose --profile tools run --rm cli
```

Запрос для CLI берётся из переменной окружения `QUERY` в `docker-compose.yml`.

## Что где хранится

- MongoDB: база `infosearch`, коллекция `articles`.
- Индекс и статистика: `task_4/output/` (`index.tsv`, `docs.tsv`, `freq.csv`, `stats.txt`).
- Веб и CLI читают индекс из `task_4/output/`.

## Импорт собственного корпуса

Индексатор ожидает коллекцию `articles` в MongoDB. Каждая запись должна содержать минимум:

- `url` (уникальный идентификатор документа),
- `text` (текст для индексирования).

Опционально можно добавить: `title`, `author`, `published_at`, `source`.

### Вариант А. JSONL → MongoDB (рекомендуется)

1. Подготовьте JSONL файл, один документ на строку. Пример строки:
```json
{"url":"https://example.org/doc1","title":"Заголовок","text":"Полный текст документа","source":"custom","published_at":"2026-03-02"}
```

2. Убедитесь, что MongoDB запущена:
```sh
docker compose up -d mongo
```

3. Установите зависимости и загрузите данные:
```sh
pip install -r task_1/requirements.txt
python task_1/load_to_mongo.py --input path/to/your.jsonl
```

По умолчанию загрузка идёт в `mongodb://admin:password@localhost:27017/?authSource=admin`, база `infosearch`, коллекция `articles`.

4. Постройте индекс и запустите поиск:
```sh
docker compose --profile tools run --rm indexer
docker compose up web
```

### Вариант Б. Скрапинг готовыми скриптами

В `task_1/scrape_articles.py` есть скрипт, который собирает статьи из Русской Википедии и Habr, сохраняет JSONL и может быть использован как пример для адаптации под свои источники. После получения JSONL используйте `task_1/load_to_mongo.py` (см. Вариант А).

## Полезные файлы

- `docker-compose.yml` - конфигурация сервисов и маршруты индекса.
- `.env` - учётные данные MongoDB (по умолчанию `admin/password`).
- `task_4/README.md` - детали индексатора.
- `task_5/README.md` - детали CLI и WEB поиска.
