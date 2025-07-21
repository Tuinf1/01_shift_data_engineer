--  создание таблицы: дата, метрики, значения, ключ
CREATE TABLE weather_metrics (
    date DATE NOT NULL,  
    metric TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY (date, metric)
);
