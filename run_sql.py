# run_sql.py

import snowflake.connector
from config import SNOWFLAKE_CONFIG

def run_sql_file(filename):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
    cursor = conn.cursor()
    with open(filename, 'r') as f:
        sql_commands = f.read().split(';')
        for command in sql_commands:
            if command.strip() != '':
                cursor.execute(command)
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_sql_file("transform_data.sql")