CREATE TABLE IF NOT EXISTS readings_weather (
    "timestamp" TIMESTAMP NOT NULL,
    "station_id" TEXT NOT NULL,
    "reading_type" TEXT NOT NULL,
    "value" NUMERIC,
    PRIMARY KEY ("timestamp", "station_id", "reading_type"),
    FOREIGN KEY ("station_id") REFERENCES stations_weather ("station_id")
);