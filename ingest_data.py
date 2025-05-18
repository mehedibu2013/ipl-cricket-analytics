# ingest_data.py

import snowflake.connector
from config import SNOWFLAKE_CONFIG


def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
    return conn


def create_tables(conn):
    cursor = conn.cursor()
    try:
        # Step 1: Create Database
        cursor.execute("CREATE DATABASE IF NOT EXISTS CRICKET_DB;")

        # Step 2: Create Schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW;")

        # Step 3: Set context
        cursor.execute("USE DATABASE CRICKET_DB;")
        cursor.execute("USE SCHEMA RAW;")
        cursor.execute("USE WAREHOUSE COMPUTE_WH;")

        # Step 4: Create MATCHES Table with 18 columns
        cursor.execute("""
            CREATE OR REPLACE TABLE MATCHES (
                id INT,
                season STRING,
                city STRING,
                date DATE,
                team1 STRING,
                team2 STRING,
                toss_winner STRING,
                toss_decision STRING,
                result STRING,
                dl_applied INT,
                winner STRING,
                win_by_runs INT,
                win_by_wickets INT,
                player_of_match STRING,
                venue STRING,
                umpire1 STRING,
                umpire2 STRING,
                umpire3 STRING
            );
        """)

        # Step 5: Create DELIVERIES Table (basic structure)
        cursor.execute("""
            CREATE OR REPLACE TABLE DELIVERIES (
                match_id INT,
                inning INT,
                batting_team STRING,
                bowling_team STRING,
                over INT,
                ball INT,
                batsman STRING,
                non_striker STRING,
                bowler STRING,
                is_super_over INT,
                wide_runs INT,
                bye_runs INT,
                legbye_runs INT,
                noball_runs INT,
                penalty_runs INT,
                batsman_runs INT,
                extra_runs INT,
                total_runs INT,
                player_dismissed STRING,
                dismissal_kind STRING,
                fielder STRING
            );
        """)

        print("Tables created successfully.")

    finally:
        cursor.close()


def upload_file_to_stage_and_table(conn, file_path, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        # Put file to internal stage
        put_query = f"PUT file://{file_path} @%{table_name}"
        cursor.execute(put_query)
        print(f"{file_path} uploaded to stage @{table_name}")

        # Copy into table with proper date formatting
        copy_query = f"""
        COPY INTO {table_name}
        FROM @%{table_name}
        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 
                     FIELD_OPTIONALLY_ENCLOSED_BY='"'
                     DATE_FORMAT='DD/MM/YY'
                     ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_query)
        print(f"{file_path} loaded into {table_name}")

    finally:
        cursor.close()

if __name__ == "__main__":
    connection = connect_to_snowflake()

    # Step 1: Create tables
    create_tables(connection)

    # Step 2: Upload matches.csv
    upload_file_to_stage_and_table(connection, "data/matches.csv", "MATCHES")

    # Step 3: Upload deliveries.csv
    upload_file_to_stage_and_table(connection, "data/deliveries.csv", "DELIVERIES")

    # Close connection
    connection.close()