TRUNCATE TABLE dwh.Fact_LapTime RESTART IDENTITY CASCADE;
TRUNCATE TABLE dwh.Dim_Driver RESTART IDENTITY CASCADE;
TRUNCATE TABLE dwh.Dim_Circuit RESTART IDENTITY CASCADE;
TRUNCATE TABLE dwh.Dim_Constructor RESTART IDENTITY CASCADE;
TRUNCATE TABLE dwh.Dim_Compound RESTART IDENTITY CASCADE;
TRUNCATE TABLE dwh.Dim_Date RESTART IDENTITY CASCADE;

INSERT INTO dwh.Dim_Driver (driver_id_bk, code, forename, surname, nationality)
SELECT DISTINCT driverid, code, givenname, familyname, nationality FROM stage.drivers;

INSERT INTO dwh.Dim_Circuit (circuit_id_bk, circuit_name, location, country)
SELECT DISTINCT circuitid, circuitname, location, country FROM stage.races;

INSERT INTO dwh.Dim_Constructor (team_name_bk)
SELECT DISTINCT teamname FROM stage.laps WHERE teamname IS NOT NULL;

INSERT INTO dwh.Dim_Compound (compound_name)
SELECT DISTINCT compound FROM stage.laps WHERE compound IS NOT NULL;

INSERT INTO dwh.Dim_Date (date_sk, full_date, year, month, day, day_name, weekend_flag)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT,
    datum,
    EXTRACT(YEAR FROM datum),
    EXTRACT(MONTH FROM datum),
    EXTRACT(DAY FROM datum),
    TO_CHAR(datum, 'Day'),
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END
FROM generate_series('2021-01-01'::date, '2025-12-31'::date, '1 day'::interval) as datum;

INSERT INTO dwh.Fact_LapTime (
    driver_sk, constructor_sk, circuit_sk, compound_sk, date_sk, 
    lap_number, lap_time_ms, stint_sequence, calculated_tyre_life
)
SELECT 
    d.driver_sk,
    c.constructor_sk,
    cir.circuit_sk,
    cmp.compound_sk,
    TO_CHAR(r.date::DATE, 'YYYYMMDD')::INT,
    l.lapnumber,
    l.laptime_ms,
    l.stint_seq,
    ROW_NUMBER() OVER (
        PARTITION BY l.year, l.racename, l.drivercode, l.stint_seq 
        ORDER BY l.lapnumber
    ) as calculated_tyre_life
FROM (
    SELECT DISTINCT 
        year, racename, drivercode, teamname, lapnumber, laptime_ms, compound, tyrelife,
        SUM(CASE 
            WHEN compound IS DISTINCT FROM prev_compound THEN 1 
            WHEN tyrelife < prev_tyrelife THEN 1 
            ELSE 0 
        END) OVER (PARTITION BY year, racename, drivercode ORDER BY lapnumber) + 1 as stint_seq
    FROM (
        SELECT *,
            LAG(compound) OVER (PARTITION BY year, racename, drivercode ORDER BY lapnumber) as prev_compound,
            LAG(tyrelife) OVER (PARTITION BY year, racename, drivercode ORDER BY lapnumber) as prev_tyrelife
        FROM stage.laps
    ) raw
) l
LEFT JOIN dwh.Dim_Driver d ON l.drivercode = d.code
LEFT JOIN dwh.Dim_Constructor c ON l.teamname = c.team_name_bk
LEFT JOIN dwh.Dim_Compound cmp ON l.compound = cmp.compound_name


LEFT JOIN (SELECT DISTINCT racename, year, date, circuitid FROM stage.races) r 
    ON l.racename = r.racename AND l.year = r.year
LEFT JOIN dwh.Dim_Circuit cir ON r.circuitid = cir.circuit_id_bk;