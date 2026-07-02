/*
===================================================================
ETL STEP: Bronze ➝ Silver Layer - Data Cleaning & Transformation
===================================================================

DESCRIPTION:
------------
This ETL script performs the cleaning and transformation of raw data ingested into the Bronze layer and loads the refined output 
into the Silver layer tables. This step ensures the data is reliable, standardized, and ready for analytical and reporting purposes.

KEY TRANSFORMATION OPERATIONS:
------------------------------
1. **Null & Invalid Value Handling**
2. **Data Enrichment**
3. **Data Standardization**
4. **Derived Columns**
5. **Format Handling & Conversion**
6. **Data Auditing & Logging**
  
OBJECTIVES:
-----------
- Improve data quality and trustworthiness.
- Enable reliable downstream processing in the Gold layer.
- Maintain traceability and auditability across ETL stages.
- Establish best practices in scalable data engineering pipelines.
*/


-- ============================================================
-- Helper: converts "M:SS.mmm" or bare seconds to DOUBLE PRECISION
-- ============================================================
CREATE OR REPLACE FUNCTION silver.qual_time_to_seconds(time_str TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
AS $$
BEGIN
    IF time_str IS NULL OR TRIM(time_str) = '' THEN
        RETURN NULL;
    END IF;
    IF POSITION(':' IN time_str) > 0 THEN
        RETURN
            SPLIT_PART(time_str, ':', 1)::INTEGER * 60 +
            SPLIT_PART(time_str, ':', 2)::DOUBLE PRECISION;
    END IF;
    RETURN time_str::DOUBLE PRECISION;
END;
$$;


-- ============================================================
-- ETL STEP: Bronze -> Silver Layer (single consolidated batch)
-- Load order respects FK dependency chain:
--   circuits -> constructors -> drivers -> schedule ->
--   telemetry -> qualifying -> laps -> pitstops -> results -> weather
-- ============================================================
CREATE OR REPLACE FUNCTION silver.load_silver()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time   TIMESTAMP;
    v_end_time     TIMESTAMP;
    v_batch_start  TIMESTAMP;
    v_batch_end    TIMESTAMP;
    v_row_count    INT;
BEGIN
    v_batch_start := clock_timestamp();

    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'Starting silver load procedure...';
    RAISE NOTICE '=======================================================';

    -- --------------------------------------------------------
    -- Truncate ALL silver tables together in one statement.
    -- Doing this individually in dependency order would still
    -- work, but truncating together + CASCADE is simpler and
    -- avoids re-deriving the order every time a table is added.
    -- --------------------------------------------------------
    RAISE NOTICE '>>>> Truncating all silver tables';
    TRUNCATE TABLE
        silver.circuits, silver.constructors, silver.drivers, silver.schedule,
        silver.telemetry, silver.qualifying, silver.laps, silver.pitstops,
        silver.results, silver.weather
    RESTART IDENTITY CASCADE;

    -- ============================================================
    -- 1. circuits (no dependencies)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.circuits';
    v_start_time := clock_timestamp();

    INSERT INTO silver.circuits (
	    circuit_ref, 
	    circuit_name, 
		city, 
		country, 
		latitude, 
		longitude
	)
		
    SELECT 
	    TRIM(circuit_ref), 
		TRIM(circuit_name), 
		TRIM(city), 
		TRIM(country),
        CAST(latitude AS FLOAT), 
		CAST(longitude AS FLOAT)
    FROM bronze.circuits
    WHERE circuit_ref  IS NOT NULL
      AND circuit_name IS NOT NULL
      AND city         IS NOT NULL
      AND country      IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 2. constructors (no dependencies)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.constructors';
    v_start_time := clock_timestamp();

    INSERT INTO silver.constructors (
	    constructor_ref, 
		constructor_name, 
		nationality
	)
	
    SELECT 
	    TRIM(constructor_ref), 
		TRIM(constructor_name), 
		TRIM(nationality)
    FROM bronze.constructors
    WHERE constructor_ref IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 3. drivers (no dependencies)
    --    - derive driver_name from forename + surname
    --    - driver_code = first 3 letters of surname
    --    - manual fill for 14 rows missing dob/nationality
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.drivers';
    v_start_time := clock_timestamp();

    INSERT INTO silver.drivers (
	    driver_ref, 
		driver_code, 
		driver_number, 
		driver_name, 
		date_of_birth, 
		nationality
	)
	
    WITH manual_fill AS (
        SELECT * FROM (VALUES
            ('Paul Aron',         '1999-02-04'::DATE, 'Estonian'),
            ('Dino Beganovic',    '2004-01-19'::DATE, 'Swedish'),
            ('Luke Browning',     '2002-01-31'::DATE, 'British'),
            ('Jak Crawford',      '2005-05-02'::DATE, 'American'),
            ('Felipe Drugovich',  '2000-05-23'::DATE, 'Brazilian'),
            ('Alexander Dunne',   '2005-11-11'::DATE, 'Irish'),
            ('Antonio Fuoco',     '1996-05-20'::DATE, 'Italian'),
            ('Ryo Hirakawa',      '1994-03-07'::DATE, 'Japanese'),
            ('Ayumu Iwasa',       '2001-09-22'::DATE, 'Japanese'),
            ('Arthur Leclerc',    '2000-10-14'::DATE, 'Monégasque'),
            ('Victor Martins',    '2001-06-16'::DATE, 'French'),
            ('Patricio O''Ward',  '1999-05-06'::DATE, 'Mexican'),
            ('Cian Shields',      '2005-03-07'::DATE, 'British'),
            ('Frederik Vesti',    '2002-01-13'::DATE, 'Danish')
        ) AS t(driver_name, dob, nat)
    )
    SELECT
        TRIM(b.driver_ref),
        UPPER(LEFT(TRIM(b.surname), 3)),
        NULLIF(TRIM(b.driver_number), '')::INTEGER,
        TRIM(b.forename) || ' ' || TRIM(b.surname),
        COALESCE(b.date_of_birth::DATE, m.dob),
        COALESCE(TRIM(b.nationality), m.nat)
    FROM bronze.drivers b
    LEFT JOIN manual_fill m
           ON (TRIM(b.forename) || ' ' || TRIM(b.surname)) = m.driver_name
    WHERE b.driver_ref IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 4. schedule (needs circuits) -- MOVED BEFORE telemetry
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.schedule';
    v_start_time := clock_timestamp();

    INSERT INTO silver.schedule (
	    season, 
		round_number, 
		race_name, 
		circuit_ref, 
		race_datetime,
        fp1_date, 
		fp2_date, 
		fp3_date, 
		quali_date, 
		has_sprint
	)
	
    SELECT
        season,
        round_number,
        TRIM(race_name),
        TRIM(circuit_ref),
        (TRIM(race_date) || ' ' || TRIM(race_time))::TIMESTAMP AT TIME ZONE 'UTC',
        TRIM(fp1_date)::DATE,
        TRIM(fp2_date)::DATE,
        TRIM(fp3_date)::DATE,
        TRIM(quali_date)::DATE,
        has_sprint
    FROM bronze.schedule
    WHERE season       IS NOT NULL
      AND round_number IS NOT NULL
      AND race_name    IS NOT NULL
      AND circuit_ref  IS NOT NULL
      AND has_sprint   IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 5. telemetry (needs schedule, drivers) 
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.telemetry';
    v_start_time := clock_timestamp();

    INSERT INTO silver.telemetry (
	    season, 
		round_number, 
		driver_ref, 
		lap_number, 
		sample_count,
		avg_speed_kph, 
		max_speed_kph, 
		min_speed_kph, 
		avg_throttle_pct,
		full_throttle_pct, 
		heavy_braking_pct, 
		drs_active_pct,
		avg_gear, 
		gear_changes, 
		avg_rpm, 
		max_rpm
	)
	
    SELECT
        season::SMALLINT,
        round_number::SMALLINT,
        TRIM(driver_id),
        lap_number::SMALLINT,
        sample_count::INTEGER,
        avg_speed_kph, 
		max_speed_kph, 
		min_speed_kph, 
		avg_throttle_pct,
        full_throttle_pct, 
		heavy_braking_pct, 
		drs_active_pct,
        avg_gear, 
		gear_changes::INTEGER, 
		avg_rpm, 
		max_rpm
    FROM bronze.telemetry
    WHERE driver_id    IS NOT NULL
      AND season       IS NOT NULL
      AND round_number IS NOT NULL
      AND lap_number   IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 6. qualifying (needs schedule, drivers, constructors)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.qualifying';
    v_start_time := clock_timestamp();

    INSERT INTO silver.qualifying (
	    season, 
		round_number, 
		driver_ref, 
		constructor_ref,
        quali_position, 
		q1_time_sec, 
		q2_time_sec, 
		q3_time_sec
	)
	
    SELECT
        season::SMALLINT,
        round_number::SMALLINT,
        TRIM(driver_ref),
        TRIM(constructor_ref),
        quali_position::SMALLINT,
        silver.qual_time_to_seconds(q1_time),
        silver.qual_time_to_seconds(q2_time),
        silver.qual_time_to_seconds(q3_time)
    FROM bronze.qualifying
    WHERE driver_ref      IS NOT NULL
      AND constructor_ref IS NOT NULL   -- added: silver.constructor_ref is NOT NULL
      AND season          IS NOT NULL
      AND round_number    IS NOT NULL
      AND quali_position  IS NOT NULL;  -- added: silver.quali_position is NOT NULL

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 7. laps (needs schedule, drivers)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.laps';
    v_start_time := clock_timestamp();

    INSERT INTO silver.laps (
	    season, 
		round_number, 
		driver_ref, 
		lap_number, 
		position, 
		lap_time_sec
	)
	
    SELECT
        season::SMALLINT,
        round_number::SMALLINT,
        driver_ref,
        lap_number::SMALLINT,
        position::SMALLINT,
        lap_time_ms / 1000.0
    FROM bronze.laps
    WHERE driver_ref  IS NOT NULL
      AND season      IS NOT NULL
      AND round_number IS NOT NULL
      AND position    IS NOT NULL
      AND lap_time_ms IS NOT NULL;  -- added: silver.lap_time_sec is NOT NULL

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 8. pitstops (needs schedule, drivers)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.pitstops';
    v_start_time := clock_timestamp();

    INSERT INTO silver.pitstops (
	    season, 
		round_number, 
		driver_ref, 
		lap_number, 
		stop_number, 
		duration_sec
	)
	
    WITH manual_fill AS (
        SELECT * FROM (VALUES
            ('max_verstappen', 2018::SMALLINT, 10::SMALLINT, 2::SMALLINT, 28000.0),
            ('hulkenberg',     2019::SMALLINT, 13::SMALLINT, 2::SMALLINT, 23000.0),
            ('bottas',         2019::SMALLINT, 17::SMALLINT, 1::SMALLINT, 23000.0),
            ('gasly',          2020::SMALLINT, 17::SMALLINT, 1::SMALLINT, 22000.0),
            ('mazepin',        2021::SMALLINT,  9::SMALLINT, 1::SMALLINT, 22000.0),
            ('tsunoda',        2024::SMALLINT, 12::SMALLINT, 1::SMALLINT, 30000.0),
            ('tsunoda',        2025::SMALLINT, 11::SMALLINT, 2::SMALLINT, 30000.0)
        ) AS t(driver_ref, season, round_number, stop_number, duration_ms)
    )
    SELECT
        p.season::SMALLINT,
        p.round_number::SMALLINT,
        TRIM(p.driver_ref),
        p.lap_number::SMALLINT,
        p.stop_number::SMALLINT,
        COALESCE(p.duration_ms, m.duration_ms) / 1000.0
    FROM bronze.pitstops AS p
    LEFT JOIN manual_fill AS m
           ON p.driver_ref   = m.driver_ref
          AND p.season       = m.season
          AND p.round_number = m.round_number
          AND p.stop_number  = m.stop_number
    WHERE p.driver_ref     IS NOT NULL
      AND p.season         IS NOT NULL
      AND p.round_number   IS NOT NULL
      AND p.lap_number     IS NOT NULL
      AND COALESCE(p.duration_ms, m.duration_ms) IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 9. results (needs schedule, drivers, constructors)
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.results';
    v_start_time := clock_timestamp();

    INSERT INTO silver.results (
	    season, 
		round_number, 
		driver_ref, 
		constructor_ref, 
		grid_position,
        finish_position, 
		points, 
		laps_completed, 
		status
	)
	
    SELECT
        season::SMALLINT,
        round_number::SMALLINT,
        TRIM(driver_ref),
        TRIM(constructor_ref),
        grid_position::SMALLINT,
        finish_position::SMALLINT,
        COALESCE(points, 0),        
        laps_completed::SMALLINT,
        COALESCE(TRIM(status), 'n/a') 
    FROM bronze.results
    WHERE driver_ref      IS NOT NULL
      AND constructor_ref IS NOT NULL  
      AND season          IS NOT NULL
      AND round_number    IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    -- 10. weather (needs schedule) 
    -- ============================================================
    RAISE NOTICE '>>>> Inserting Data Into: silver.weather';
    v_start_time := clock_timestamp();

    INSERT INTO silver.weather (
	    season, 
		round_number, 
		session_time_ms, 
		air_temp_c, 
		track_temp_c,
        humidity_pct, 
		pressure_mbar, 
		wind_speed_ms, 
		wind_direction_deg, 
		rainfall)
    SELECT
        season::INT,
        round_number::INT,
        session_time_ms,
        air_temp_c, 
		track_temp_c, 
		humidity_pct, 
		pressure_mbar,
        wind_speed_ms, 
		wind_direction_deg, 
		rainfall
    FROM bronze.weather
    WHERE season           IS NOT NULL
      AND round_number     IS NOT NULL
      AND session_time_ms  IS NOT NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Rows loaded: % | Duration: % sec', v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);

    -- ============================================================
    v_batch_end := clock_timestamp();
    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'Silver load complete.';
    RAISE NOTICE 'Total batch duration: % seconds',
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start))::NUMERIC(10,2);
    RAISE NOTICE '=======================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'Error occurred during silver.load_silver()';
    RAISE NOTICE 'Error message: %', SQLERRM;
    RAISE NOTICE '=======================================================';
END;
$$;

SELECT silver.load_silver();