# Shift Data Engineer — Weather ETL

ETL-скрипт для получения погодных данных из [Open-Meteo API](https://open-meteo.com/), обработки и сохранения в `.csv`.

## 🚀 Стек

- Python 3.x
- Pandas
- NumPy
- Requests

## 📦 Установка

```bash
git clone https://github.com/Tuinf1/01_shift_data_engineer.git
cd 01_shift_data_engineer
pip install -r requirements.txt

docker-compose up -d
python app/weather_etl.py


Обоснование архитектуры и решений

1. Получение данных (fetch_data)
Используется Open-Meteo API с параметрами:

hourly — для получения погодных метрик по часам

daily — для рассчёта светового дня (sunrise/sunset)

timeformat=unixtime упрощает преобразование в datetime

Аргументы:

API не требует токена, стабилен и бесплатен

Единый запрос сразу покрывает все метрики


2. Агрегация данных 
📌 aggregate_24h
Агрегация по суткам через .resample("1D"):

mean для температуры, влажности и т.п.

sum для осадков

📌 aggregate_daylight
Агрегация только в интервале [sunrise, sunset] — это даёт отдельный срез метрик за световой день, что важно для:

оценки комфорта

анализа воздействия на организм/поведение

Аргументы:

.resample() и loc[start:end] — быстрые и надёжные при больших объёмах

такой подход лучше, чем интерполяция по "дневным" часам