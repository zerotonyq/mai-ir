# Task 2: C++ Crawler

## Запуск

1. В корне проекта:

```bash
# Поднимет MongoDB, mongo-express и crawler
# (crawler читает /app/config.yaml)
docker compose up --build
```

2. Конфиг:
- `task_2/config.yaml`
- Единственный аргумент приложения: путь до yaml-конфига.

## Что хранится в MongoDB
Коллекция `pages` содержит поля:
- `url` (нормализованный)
- `html` (сырой HTML)
- `source` (название источника)
- `fetched_at` (Unix timestamp)

Дополнительно:
- `hash` (для проверки изменений)
- `last_checked` (если документ не изменился)

## Возобновление
Очередь хранится в коллекции `queue`. При перезапуске все записи со статусом `processing` переводятся в `pending`.
