# dashboard.py

import streamlit as st
import snowflake.connector
import pandas as pd
from config import SNOWFLAKE_CONFIG


def get_snowflake_data(query):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema="STAGING"
    )
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        return df
    finally:
        conn.close()


# Streamlit UI
st.set_page_config(page_title="🏏 Cricket Analytics Dashboard", layout="wide")
st.title("🏏 IPL Cricket Analytics Dashboard")

st.markdown("Explore top teams and batsmen performance based on historical match data.")

# Section 1: Top Winning Teams
st.subheader("🏆 Top 10 Winning Teams")
df_top_teams = get_snowflake_data("SELECT * FROM STAGING.MOST_WINNING_TEAMS")

if not df_top_teams.empty:
    st.bar_chart(df_top_teams.set_index('WINNER'))
    st.dataframe(df_top_teams)
else:
    st.warning("No data found for winning teams.")

# Section 2: Top Batsmen by Batting Average
st.subheader("🔥 Top 10 Batsmen by Batting Average")
df_batsmen = get_snowflake_data("SELECT * FROM STAGING.BATSMAN_PERFORMANCE")

# Debug: Print column names
st.write(df_batsmen.columns.tolist())

if not df_batsmen.empty:
    # Use actual column names returned by Snowflake
    st.line_chart(df_batsmen.set_index('BATSMAN')['BATTING_AVG'])
    st.dataframe(df_batsmen)
else:
    st.warning("No data found for batsmen performance.")