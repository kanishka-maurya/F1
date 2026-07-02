/* 
===================================================================================
Defining Tables within "silver" Database using DDL.

Schema of a table must match the data definition of the source data to retain data 
without loss, truncation, or type mismatches during loading or migration processes.
===================================================================================
*/


CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.circuits CASCADE;
CREATE TABLE IF NOT EXISTS silver.circuits (
    circuit_ref   VARCHAR(50)  PRIMARY KEY,
    circuit_name  VARCHAR(100) NOT NULL,
    city          VARCHAR(50)  NOT NULL,
    country       VARCHAR(50)  NOT NULL,
    latitude      FLOAT        NOT NULL,
    longitude     FLOAT        NOT NULL
);


DROP TABLE IF EXISTS silver.constructors CASCADE;
CREATE TABLE IF NOT EXISTS silver.constructors (
    constructor_ref   VARCHAR(50)  PRIMARY KEY,
    constructor_name  VARCHAR(100) NOT NULL,
    nationality       VARCHAR(50)  NOT NULL
);


DROP TABLE IF EXISTS silver.drivers CASCADE;
CREATE TABLE IF NOT EXISTS silver.drivers (
    driver_ref     VARCHAR(50)  PRIMARY KEY,
    driver_code    VARCHAR(3)   NOT NULL,
    driver_number  INTEGER      NULL,        
    driver_name    VARCHAR(100) NOT NULL,
    date_of_birth  DATE         NOT NULL,
    nationality    VARCHAR(50)  NOT NULL
);


DROP TABLE IF EXISTS silver.schedule CASCADE;
CREATE TABLE silver.schedule (
    schedule_id     BIGSERIAL   PRIMARY KEY,
    season          BIGINT      NOT NULL,
    round_number    BIGINT      NOT NULL,
    race_name       TEXT        NOT NULL,
	circuit_ref     TEXT        NOT NULL,
    race_datetime   TIMESTAMP   WITH TIME ZONE NOT NULL,
    fp1_date        DATE,
    fp2_date        DATE,
    fp3_date        DATE,
    quali_date      DATE,
    has_sprint      BOOLEAN     NOT NULL,

	CONSTRAINT fk_schedule_circuit FOREIGN KEY (circuit_ref) REFERENCES silver.circuits(circuit_ref),
    CONSTRAINT unique_schedule UNIQUE (season, round_number)
);


DROP TABLE IF EXISTS silver.telemetry;
CREATE TABLE IF NOT EXISTS silver.telemetry (
    tele_id           BIGSERIAL    PRIMARY KEY,
    season            SMALLINT     NOT NULL,
    round_number      SMALLINT     NOT NULL,
    driver_ref        VARCHAR(50)  NOT NULL REFERENCES silver.drivers(driver_ref),
    lap_number        SMALLINT     NOT NULL,
    sample_count      INTEGER      NULL,
    avg_speed_kph     FLOAT        NULL,
    max_speed_kph     FLOAT        NULL,
    min_speed_kph     FLOAT        NULL,
    avg_throttle_pct  FLOAT        NULL,
    full_throttle_pct FLOAT        NULL,
    heavy_braking_pct FLOAT        NULL,
    drs_active_pct    FLOAT        NULL,
    avg_gear          FLOAT        NULL,
    gear_changes      INTEGER      NULL,
    avg_rpm           FLOAT        NULL,
    max_rpm           FLOAT        NULL,

    CONSTRAINT fk_tele_schedule FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    CONSTRAINT fk_tele_driver FOREIGN KEY (driver_ref) REFERENCES silver.drivers(driver_ref),
	CONSTRAINT unique_telemetry UNIQUE (season, round_number, driver_ref, lap_number)	                         
);


DROP TABLE IF EXISTS silver.qualifying;
CREATE TABLE silver.qualifying (
    qual_id              BIGSERIAL        PRIMARY KEY,
    season               SMALLINT         NOT NULL,
    round_number         SMALLINT         NOT NULL,
    driver_ref           TEXT             NOT NULL,
    constructor_ref      TEXT             NOT NULL,
    quali_position       SMALLINT         NOT NULL,
    q1_time_sec          DOUBLE PRECISION,
    q2_time_sec          DOUBLE PRECISION,
    q3_time_sec          DOUBLE PRECISION,

	CONSTRAINT fk_qual_schedule FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    CONSTRAINT fk_qual_driver FOREIGN KEY (driver_ref) REFERENCES silver.drivers(driver_ref),
    CONSTRAINT fk_qual_constructor FOREIGN KEY (constructor_ref) REFERENCES silver.constructors(constructor_ref),
    CONSTRAINT unique_qual UNIQUE (season, round_number, driver_ref)
);


DROP TABLE IF EXISTS silver.laps;
CREATE TABLE silver.laps (
    laps_id         BIGSERIAL      PRIMARY KEY,
    season          SMALLINT         NOT NULL,
    round_number    SMALLINT         NOT NULL,
    driver_ref      VARCHAR(50)      NOT NULL,
    lap_number      SMALLINT         NOT NULL,
    position        SMALLINT         NOT NULL,
    lap_time_sec    DOUBLE PRECISION NOT NULL,

	CONSTRAINT fk_laps_schedule FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    CONSTRAINT fk_laps_driver FOREIGN KEY (driver_ref) REFERENCES silver.drivers(driver_ref),
    CONSTRAINT unique_laps UNIQUE (season, round_number, driver_ref, lap_number)
);


DROP TABLE IF EXISTS silver.pitstops;
CREATE TABLE silver.pitstops (
    pitstop_id    BIGSERIAL         PRIMARY KEY,
    season        SMALLINT          NOT NULL,
    round_number  SMALLINT          NOT NULL,
    driver_ref    VARCHAR(50)       NOT NULL,
	lap_number    SMALLINT          NOT NULL,
    stop_number   SMALLINT          NOT NULL,
    duration_sec  DOUBLE PRECISION  NOT NULL,

    CONSTRAINT fk_pitstops_schedule FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    CONSTRAINT fk_pitstops_driver FOREIGN KEY (driver_ref) REFERENCES silver.drivers(driver_ref),
    CONSTRAINT unique_pitstops UNIQUE (season, round_number, driver_ref, stop_number)
);


DROP TABLE IF EXISTS silver.results;
CREATE TABLE silver.results (
    result_id               BIGSERIAL     PRIMARY KEY,
    season                  SMALLINT          NOT NULL,
    round_number            SMALLINT          NOT NULL,
    driver_ref              VARCHAR(50)       NOT NULL,
    constructor_ref         VARCHAR(50)       NOT NULL,
    grid_position           SMALLINT          NULL,
    finish_position         SMALLINT          NULL,
    points                  DOUBLE PRECISION  NOT NULL,
    laps_completed          SMALLINT          NULL,
    status                  VARCHAR(100)      NOT NULL,

	CONSTRAINT fk_results_schedule FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    CONSTRAINT fk_results_driver FOREIGN KEY (driver_ref) REFERENCES silver.drivers(driver_ref),
    CONSTRAINT fk_results_constructor FOREIGN KEY (constructor_ref) REFERENCES silver.constructors(constructor_ref),
    CONSTRAINT unique_results UNIQUE (season, round_number, driver_ref)
);


DROP TABLE IF EXISTS silver.weather;
CREATE TABLE silver.weather (
    weather_reading_id  BIGSERIAL PRIMARY KEY,
    season              INT NOT NULL,
    round_number        INT NOT NULL,
    session_time_ms     BIGINT NOT NULL,
    air_temp_c          DOUBLE PRECISION,
    track_temp_c        DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    pressure_mbar       DOUBLE PRECISION,
    wind_speed_ms       DOUBLE PRECISION,
    wind_direction_deg  DOUBLE PRECISION,
    rainfall            BOOLEAN,
    FOREIGN KEY (season, round_number) REFERENCES silver.schedule(season, round_number),
    UNIQUE (season, round_number, session_time_ms)
);