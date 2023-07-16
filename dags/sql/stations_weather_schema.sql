CREATE TABLE IF NOT EXISTS stations_weather (
    "station_id" TEXT PRIMARY KEY,
    "device_id" TEXT,
    "name" TEXT,
    "longitude" NUMERIC,
    "latitude" NUMERIC
);