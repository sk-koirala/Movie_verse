import os
import psycopg2
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Movies ETL Dashboard", layout="wide")

DB_HOST=os.getenv("DB_HOST","localhost")
DB_PORT=int(os.getenv("DB_PORT","5432"))
DB_NAME=os.getenv("DB_NAME","movies")
DB_USER=os.getenv("DB_USER","etl_user")
DB_PASS=os.getenv("DB_PASS","etl_pass")

@st.cache_data(ttl=60)
def query(sql):
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS) as conn:
        return pd.read_sql(sql, conn)

st.title("🎬 Movies ETL Dashboard")
st.caption("Powered by PySpark + PostgreSQL")

tab1, tab2, tab3 = st.tabs(["Genres", "Yearly Trend", "Top Movies"])

with tab1:
    st.subheader("Ratings by Genre")
    df = query("SELECT genre, n_ratings, avg_rating FROM genre_stats ORDER BY n_ratings DESC LIMIT 50;")
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("genre")["n_ratings"])
    st.line_chart(df.set_index("genre")["avg_rating"])

with tab2:
    st.subheader("Ratings over Years")
    df = query("SELECT year, n_ratings, avg_rating FROM year_stats WHERE year IS NOT NULL ORDER BY year;")
    st.dataframe(df, use_container_width=True)
    st.line_chart(df.set_index("year")[["n_ratings","avg_rating"]])

with tab3:
    st.subheader("Top Movies by Avg Rating (min reviews)")
    min_reviews = st.slider("Minimum number of ratings", 5, 200, 10, step=5)
    df = query(f"SELECT movie_name, n_ratings, avg_rating FROM top_movies WHERE n_ratings >= {min_reviews} ORDER BY avg_rating DESC, n_ratings DESC LIMIT 100;")
    st.dataframe(df, use_container_width=True)
