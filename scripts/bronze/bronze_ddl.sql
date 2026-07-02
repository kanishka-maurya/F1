/* 
====================================================================================
Defining Tables within "bronze" Layer using DDL.

Schema of a table must match the data definition of the source data to retain data 
without loss, truncation, or type mismatches during loading or migration processes.
====================================================================================
*/


CREATE SCHEMA IF NOT EXISTS bronze;


DROP TABLE IF EXISTS bronze.circuits;
CREATE TABLE bronze.circuits (
    circuit_ref   TEXT,
    circuit_name  TEXT,
    city          TEXT,
    country       TEXT,
    latitude      TEXT,
    longitude     TEXT,
    altitude_m    TEXT
);


DROP TABLE IF EXISTS bronze.constructors;
CREATE TABLE bronze.constructors (
    constructor_ref   TEXT,
    constructor_name  TEXT,
    nationality       TEXT
);


DROP TABLE IF EXISTS bronze.drivers;
CREATE TABLE bronze.drivers (
    driver_ref     TEXT,
    driver_code    TEXT,
    driver_number  TEXT,
    forename       TEXT,
    surname        TEXT,
    date_of_birth  TEXT,
    nationality    TEXT
);


DROP TABLE IF EXISTS bronze.schedule;
CREATE TABLE bronze.schedule (
    season        BIGINT,
    round_number  BIGINT,
    race_name     TEXT,
    circuit_ref   TEXT,
    race_date     TEXT,
    race_time     TEXT,
    fp1_date      TEXT,
    fp2_date      TEXT,
    fp3_date      TEXT,
    quali_date    TEXT,
    sprint_date   TEXT,
    has_sprint    BOOLEAN
);


DROP TABLE IF EXISTS bronze.laps;
CREATE TABLE bronze.laps (
    season        BIGINT,
    round_number  BIGINT,
    race_name     TEXT,
    circuit_ref   TEXT,
    race_date     TEXT,
    driver_ref    TEXT,
    lap_number    SMALLINT,
    position      SMALLINT,
    lap_time_ms   DOUBLE PRECISION
);


DROP TABLE IF EXISTS bronze.pitstops;
CREATE TABLE bronze.pitstops (
    season                BIGINT,
    round_number          BIGINT,
    race_name             TEXT,
    circuit_ref           TEXT,
    race_date             TEXT,
    driver_ref            TEXT,
    stop_number           SMALLINT,
    lap_number            SMALLINT,
    local_time            TEXT,
    duration_ms           DOUBLE PRECISION,
	total_stops_race      SMALLINT 
);


DROP TABLE IF EXISTS bronze.qualifying;
CREATE TABLE bronze.qualifying (
    season          BIGINT,
    round_number    BIGINT,
    race_name       TEXT,
    circuit_ref     TEXT,
    race_date       TIMESTAMP,
    driver_ref      TEXT,
    driver_code     TEXT,
    driver_number   BIGINT,
    constructor_ref TEXT,
    quali_position  BIGINT,
    q1_time         TEXT,
    q2_time         TEXT,
    q3_time         TEXT
);


DROP TABLE IF EXISTS bronze.results;
CREATE TABLE bronze.results (
    season                   BIGINT,
    round_number             BIGINT,
    race_name                TEXT,
    circuit_ref              TEXT,
    race_date                TEXT,
    driver_ref               TEXT,
    driver_code              TEXT,
    driver_number            BIGINT,
    constructor_ref          TEXT,
    grid_position            BIGINT,
    finish_position          BIGINT,
    position_text            TEXT,
    points                   DOUBLE PRECISION,
    laps_completed           BIGINT,
    status                   TEXT,
    race_time_millis         DOUBLE PRECISION
);


DROP TABLE IF EXISTS bronze.telemetry;
CREATE TABLE bronze.telemetry (
    season             BIGINT,
    round_number       BIGINT,
    race_name          TEXT,
    circuit_ref        TEXT,
    race_date          TEXT,
    driver_number      TEXT,
    driver_id          TEXT,
    lap_number         BIGINT,
    sample_count       BIGINT,
    avg_speed_kph      DOUBLE PRECISION,
    max_speed_kph      DOUBLE PRECISION,
    min_speed_kph      DOUBLE PRECISION,
    avg_throttle_pct   DOUBLE PRECISION,
    full_throttle_pct  DOUBLE PRECISION,
    heavy_braking_pct  DOUBLE PRECISION,
    drs_active_pct     DOUBLE PRECISION,
    avg_gear           DOUBLE PRECISION,
    gear_changes       BIGINT,
    avg_rpm            BIGINT,
    max_rpm            BIGINT
);


DROP TABLE IF EXISTS bronze.weather;
CREATE TABLE bronze.weather (
    season                  BIGINT,
    round_number            BIGINT,
    race_name               TEXT,
    circuit_ref             TEXT,
    session_time_ms         BIGINT,
    air_temp_c              DOUBLE PRECISION,
    track_temp_c            DOUBLE PRECISION,
    humidity_pct            DOUBLE PRECISION,
    pressure_mbar           DOUBLE PRECISION,
    wind_speed_ms           DOUBLE PRECISION,
    wind_direction_deg      DOUBLE PRECISION,
    rainfall                BOOLEAN
);


