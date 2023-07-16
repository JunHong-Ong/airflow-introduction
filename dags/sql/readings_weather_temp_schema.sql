DROP TABLE IF EXISTS readings_weather_temp;
CREATE TABLE readings_weather_temp (
    "timestamp" TIMESTAMP NOT NULL,
    "station_id" TEXT NOT NULL,
    "reading_type" TEXT NOT NULL,
    "value" NUMERIC
);