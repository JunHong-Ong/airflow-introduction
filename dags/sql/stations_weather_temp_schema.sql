DROP TABLE IF EXISTS stations_weather_temp;
CREATE TABLE stations_weather_temp (
    "station_id" TEXT,
    "device_id" TEXT,
    "name" TEXT,
    "longitude" NUMERIC,
    "latitude" NUMERIC
);