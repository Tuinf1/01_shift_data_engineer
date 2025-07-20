CREATE TABLE IF NOT EXISTS weather_metrics (
    date DATE NOT NULL,
    metric TEXT NOT NULL,
    value TEXT, 
    PRIMARY KEY (date, metric)
);