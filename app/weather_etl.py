import requests
import pandas as pd
import numpy as np
import json
from db_insert import insert_to_db

# Запрос данных из Open-Meteo API
def fetch_data(lat, lon, start, end):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join([
            "temperature_2m", "relative_humidity_2m", "dew_point_2m",
            "apparent_temperature", "temperature_80m", "temperature_120m",
            "wind_speed_10m", "wind_speed_80m", "visibility",
            "soil_temperature_0cm", "soil_temperature_6cm",
            "rain", "showers", "snowfall"
        ]),
        "daily": "sunrise,sunset",
        "timezone": "auto",
        "timeformat": "unixtime",
        "wind_speed_unit": "ms",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm"
    }
    return requests.get(url, params=params).json()

# Агрегация метрики по суткам
def aggregate_24h(data, metric, agg_type="mean"):
    time = pd.to_datetime(data["hourly"]["time"], unit="s")
    values = np.array(data["hourly"][metric], dtype=float)
    df = pd.DataFrame({metric: values}, index=time)
    if agg_type == "sum":
        return df.resample("1D").sum()[metric].round(6).tolist()
    return df.resample("1D").mean()[metric].round(6).tolist()

# Агрегация метрики только по световому дню
def aggregate_daylight(data, metric, agg_type="mean"):
    time = pd.to_datetime(data["hourly"]["time"], unit="s")
    values = np.array(data["hourly"][metric], dtype=float)
    df = pd.DataFrame({metric: values}, index=time)

    sunrise = pd.to_datetime(data["daily"]["sunrise"], unit="s")
    sunset = pd.to_datetime(data["daily"]["sunset"], unit="s")

    daily = []
    for start, end in zip(sunrise, sunset):
        slice_ = df.loc[start:end]
        if agg_type == "sum":
            daily.append(slice_[metric].sum() if not slice_.empty else 0.0)
        else:
            daily.append(slice_[metric].mean() if not slice_.empty else np.nan)
    return np.round(daily, 6).tolist()

def main():
    lat, lon = 55.0, 83.0
    start, end = "2025-05-16", "2025-05-30"
    data = fetch_data(lat, lon, start, end)

    # Список дат (по восходу солнца), используется как индекс
    time = pd.to_datetime(data["daily"]["sunrise"], unit="s").normalize().strftime("%Y-%m-%d").tolist()
    rows = [("time", json.dumps(time))]

    # Базовые метрики: средние и суммарные по суткам и световому дню
    base_metrics = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "temperature_80m", "temperature_120m",
        "wind_speed_10m", "wind_speed_80m", "visibility",
        "rain", "showers", "snowfall"
    ]

    extra_metrics = [
        "soil_temperature_0cm", "soil_temperature_6cm"
    ]

    for m in base_metrics:
        agg_type = "sum" if m in ["rain", "showers", "snowfall"] else "mean"
        daily_mean = aggregate_24h(data, m, agg_type)
        daylight_mean = aggregate_daylight(data, m, agg_type)
        prefix = "avg" if agg_type == "mean" else "total"
        rows.append((f"{prefix}_{m}_24h", json.dumps(daily_mean)))
        rows.append((f"{prefix}_{m}_daylight", json.dumps(daylight_mean)))

    # Переименование метрик с единицами измерения
    metric_map = {
        "wind_speed_10m": "wind_speed_10m_m_per_s",
        "wind_speed_80m": "wind_speed_80m_m_per_s",
        "temperature_2m": "temperature_2m_celsius",
        "apparent_temperature": "apparent_temperature_celsius",
        "temperature_80m": "temperature_80m_celsius",
        "temperature_120m": "temperature_120m_celsius",
        "soil_temperature_0cm": "soil_temperature_0cm_celsius",
        "soil_temperature_6cm": "soil_temperature_6cm_celsius",
        "rain": "rain_mm",
        "showers": "showers_mm",
        "snowfall": "snowfall_mm"
    }

    for orig, renamed in metric_map.items():
        agg_type = "sum" if orig in ["rain", "showers", "snowfall"] else "mean"
        values = aggregate_24h(data, orig, agg_type)
        rows.append((renamed, json.dumps(values)))

    # ISO-формат времени и длительность светового дня
    sunrise_iso = pd.to_datetime(data["daily"]["sunrise"], unit="s").strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    sunset_iso = pd.to_datetime(data["daily"]["sunset"], unit="s").strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    daylight_hours = (
        (pd.to_datetime(data["daily"]["sunset"], unit="s") - 
         pd.to_datetime(data["daily"]["sunrise"], unit="s")).total_seconds() / 3600
    ).round(3).tolist()
    rows.append(("daylight_hours", json.dumps(daylight_hours)))
    rows.append(("sunrise_iso", json.dumps(sunrise_iso)))
    rows.append(("sunset_iso", json.dumps(sunset_iso)))
    
    # Удаляем временное поле
    rows = [r for r in rows if r[0] != "time"]

    # Сохраняем в CSV
    with open("weather_metrics_full.csv", "w", encoding="utf-8-sig") as f:
        f.write("metric,values\n")
        for metric, values in rows:
            f.write(f"{metric},{values}\n")
    print("Готово: weather_metrics_full.csv")
    # Подготовка списка дат
    dates = pd.to_datetime(data["daily"]["sunrise"], unit="s").normalize().strftime("%Y-%m-%d").tolist()

    # Сохр. PostgreSQL
    insert_to_db(rows, dates)

    

if __name__ == "__main__":
    main()
