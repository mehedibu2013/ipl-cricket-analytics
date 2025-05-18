-- transform_data.sql

-- Set context
USE DATABASE CRICKET_DB;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: Create STAGING schema if not exists
CREATE SCHEMA IF NOT EXISTS CRICKET_DB.STAGING;

-- Step 2: Top Winning Teams
CREATE OR REPLACE TABLE CRICKET_DB.STAGING.MOST_WINNING_TEAMS AS
SELECT winner, COUNT(*) AS win_count
FROM CRICKET_DB.RAW.MATCHES
WHERE winner IS NOT NULL
GROUP BY winner
ORDER BY win_count DESC;

-- Step 3: Batsman Performance
CREATE OR REPLACE TABLE CRICKET_DB.STAGING.BATSMAN_PERFORMANCE AS
WITH batsman_scores AS (
    SELECT
        batsman,
        SUM(batsman_runs) AS total_runs_scored,
        COUNT(DISTINCT match_id) AS matches_played
    FROM CRICKET_DB.RAW.DELIVERIES
    GROUP BY batsman
),
dismissals AS (
    SELECT
        player_dismissed,
        COUNT(*) AS dismissals_count
    FROM CRICKET_DB.RAW.DELIVERIES
    WHERE dismissal_kind IN ('caught', 'bowled', 'lbw', 'stumped')
    GROUP BY player_dismissed
)
SELECT
    b.batsman,
    b.total_runs_scored,
    COALESCE(d.dismissals_count, 0) AS dismissals_count,
    ROUND(b.total_runs_scored / NULLIF(COALESCE(d.dismissals_count, 1), 0), 2) AS batting_avg
FROM batsman_scores b
LEFT JOIN dismissals d ON b.batsman = d.player_dismissed
ORDER BY batting_avg DESC
LIMIT 10;