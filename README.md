# ETL



## Стек

- Python 3.x
- Pandas
- NumPy
- Requests

## Установка

```bash
git clone https://github.com/Tuinf1/01_shift_data_engineer.git
cd 01_shift_data_engineer
pip install -r requirements.txt


docker-compose up -d
python app/weather_etl.py --start 2025-06-01 --end 2025-06-07

# удаление 
docker-compose down -v


Структура
.
├── docker-compose.yml           # запуск PostgreSQL
├── init.sql                     # создание таблицы
├── requirements.txt
├── weather_metrics_full.csv     # выгрузка в csv
├── README.md
├── show.py                      # просмотр PostgreSQL
└── app/
    ├── weather_etl.py           # основной ETL
    └── db_insert.py             # вставка в БД
```
## 1 Задание
```bash
ETL-скрипт для получения погодных данных из [Open-Meteo API](https://open-meteo.com/), обработки и сохранения в `.csv`.

Краткое описание:

Получение данных 
Используется Open-Meteo API с параметрами.

hourly — для получения погодных метрик по часам

daily — для рассчёта светового дня (sunrise/sunset)

Метрики агрегируются по дням (24h) и по световому дню (daylight)
Имена признаков генерируются автоматически (например, avg_temperature_2m_24h, total_rain_daylight)
Каждая строка содержит имя метрики и список значений (в формате JSON)

API не требует токена, стабилен и бесплатен

Единый запрос сразу покрывает все метрики
```
## 2 Задание
```bash

Сохранение в базу данных в  PostgreSQL через docker-compose и SQL-инициализацию.

docker-compose.yml поднимает контейнер с PostgreSQL и создаёт базу weather

init.sql автоматически создаёт таблицу weather_metrics с полями:
date DATE;
metric TEXT;
value TEXT.

Это позволяет хранить как числовые метрики, так и строки (ISO-время восхода/заката).
Вставка реализована через psycopg2, данные сохраняются из weather_etl.py.


Защита от дубликатов
В таблице weather_metrics задан PRIMARY KEY (date, metric)

При вставке используется ON CONFLICT DO NOTHING, что исключает повторную загрузку одних и тех же значений

Это обеспечивает идемпотентность: скрипт можно запускать многократно без риска дублирования

Структура проекта:


```

## 3 Задание
```bash

реализация через функцию def parse_args() в weather_etl.py


пример запуска: 

python app/weather_etl.py --start 2025-06-01 --end 2025-06-07

Просмотр таблицы в PostgreSQL
docker exec -it weather-db psql -U user -d weather
\d weather_metrics
либо через show.py
