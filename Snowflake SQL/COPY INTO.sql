USE ROLE ACCOUNTADMIN;
USE DATABASE DOT_FLIGHTS;
USE SCHEMA RAW;
USE WAREHOUSE DOT_FLIGHTS_WH;

-- ============================================================
-- COPY INTO: load the .csv.gz file from S3 into RAW_FLIGHTS
-- ============================================================
COPY INTO RAW_FLIGHTS (

-- All 110 source columns + the trailing empty column = 111 columns from CSV.
    -- The 2 metadata columns (_SOURCE_FILE, _LOAD_TIMESTAMP) are populated separately.
    "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
    "Reporting_Airline", "DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Tail_Number", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "Origin",
    "OriginCityName", "OriginState", "OriginStateFips", "OriginStateName", "OriginWac",
    "DestAirportID", "DestAirportSeqID", "DestCityMarketID", "Dest",
    "DestCityName", "DestState", "DestStateFips", "DestStateName", "DestWac",
    "CRSDepTime", "DepTime", "DepDelay", "DepDelayMinutes", "DepDel15",
    "DepartureDelayGroups", "DepTimeBlk", "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn",
    "CRSArrTime", "ArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15",
    "ArrivalDelayGroups", "ArrTimeBlk",
    "Cancelled", "CancellationCode", "Diverted",
    "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", "Distance", "DistanceGroup",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "FirstDepTime", "TotalAddGTime", "LongestAddGTime",
    "DivAirportLandings", "DivReachedDest", "DivActualElapsedTime", "DivArrDelay", "DivDistance",
    "Div1Airport", "Div1AirportID", "Div1AirportSeqID", "Div1WheelsOn",
    "Div1TotalGTime", "Div1LongestGTime", "Div1WheelsOff", "Div1TailNum",
    "Div2Airport", "Div2AirportID", "Div2AirportSeqID", "Div2WheelsOn",
    "Div2TotalGTime", "Div2LongestGTime", "Div2WheelsOff", "Div2TailNum",
    "Div3Airport", "Div3AirportID", "Div3AirportSeqID", "Div3WheelsOn",
    "Div3TotalGTime", "Div3LongestGTime", "Div3WheelsOff", "Div3TailNum",
    "Div4Airport", "Div4AirportID", "Div4AirportSeqID", "Div4WheelsOn",
    "Div4TotalGTime", "Div4LongestGTime", "Div4WheelsOff", "Div4TailNum",
    "Div5Airport", "Div5AirportID", "Div5AirportSeqID", "Div5WheelsOn",
    "Div5TotalGTime", "Div5LongestGTime", "Div5WheelsOff", "Div5TailNum",
    "_TRAILING_EMPTY_COL",
    "_SOURCE_FILE", "_LOAD_TIMESTAMP"
)
FROM (
-- SELECT from stage lets us map each CSV column to a target column,
-- and also inject metadata values via METADATA$FILENAME etc.
SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
        $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
        $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
        $61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
        $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
        $81, $82, $83, $84, $85, $86, $87, $88, $89, $90,
        $91, $92, $93, $94, $95, $96, $97, $98, $99, $100,
        $101, $102, $103, $104, $105, $106, $107, $108, $109, $110,
        METADATA$FILENAME,          -- S3 SOURCE FILE PATH
        CURRENT_TIMESTAMP()         -- LOAD TIMESTAMP
    FROM @S3_DOT_FLIGHTS_STAGE
)
PATTERN = '.*on_time_.*\.csv\.gz'   -- Only match .csv.gz files (skip the .zip)
FILE_FORMAT = (
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE'
FORCE = FALSE; 
