import streamlit as st
import pandas as pd
import psycopg2 as psql # type: ignore
from plotly import graph_objects as go

st.set_page_config(
    page_title='Market 5mly Microstructure',
    page_icon="🌙"
)

@st.cache_data
def pinging_database():
    conn = psql.connect(
        database = st.secrets['DATABASE_NAME']
        , host = st.secrets['DATABASE_HOST']
        , port = int(st.secrets['DATABASE_PORT'])
        , user = st.secrets['SQL_USERNAME']
        , password = st.secrets['SQL_PASSWORD']
    )
    schema = st.secrets['SQL_SCHEMA']
    source = 'de10_rl_test348329' #st.secrets['DATA_SOURCE']
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {schema}.{source};')
    data = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data, columns=cols)
    conn.close()
    df.sort_values('unix_time', ascending=False, inplace=True)
    return df

@st.cache_data
def fetch_recent(df):
    df2 = df.copy()
    df2['time_stamp'] = pd.to_datetime(df['unix_time'], unit='s')
    return df2.sort_values('unix_time', ascending=False, inplace=False).head(2016)


def main() -> None:
    df = pinging_database()
    st.title('5 minute-ly timeseries data: ')
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download",
        data=csv,
        file_name='my_data.csv',
        mime='text/csv'
    )
    recent = fetch_recent(df)
    x = recent['time_stamp']
    y = (recent['avg_high_price'] + recent['avg_low_price'])//2
    fig = go.Figure(data=go.Scatter(x=x, y=y, mode='lines+markers'))
    st.plotly_chart(fig)

if __name__ == '__main__':
    main()

