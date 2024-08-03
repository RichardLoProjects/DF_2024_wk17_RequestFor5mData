import streamlit as st
import pandas as pd
import psycopg2 as psql # type: ignore

@st.cache_data
def pinging_database():
    conn = psql.connect(
        database = st.secrets['DATABASE_NAME']
        , host = st.secrets['DATABASE_HOST']
        , port = int(st.secrets['DATABASE_PORT'])
        , user = st.secrets['SQL_USERNAME']
        , password = st.secrets['SQL_PASSWORD']
    )
    s_name = st.secrets['SQL_TABLENAME_FOR_STATIC_DATA']
    d_name = st.secrets['SQL_TABLENAME_FOR_DYNAMIC_DATA']
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {s_name};')
    s_data = cur.fetchall()
    s_cols = [desc[0] for desc in cur.description]
    s_df = pd.DataFrame(s_data, columns=s_cols)
    cur.execute(f'SELECT * FROM {d_name};')
    d_data = cur.fetchall()
    d_cols = [desc[0] for desc in cur.description]
    d_df = pd.DataFrame(d_data, columns=d_cols)
    conn.close()
    return s_df, d_df



def main() -> None:
    pass

if __name__ == '__main__':
    main()

