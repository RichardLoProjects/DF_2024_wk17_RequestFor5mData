# DF_2024_wk17_RequestFor5mData
# Data Collection for item 13190

### Link

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://20240803-item13190-market-microstructure-5minly.streamlit.app/)

### Motivation

When using the [source API](https://prices.runescape.wiki/api/v1/osrs/timeseries?timestep=5m&id=13190) for item 13190, you may only retrieve 365 data points which corresponds to about 1-2 days worth of data. The stakeholder wishes to extend the scope of data being held.

### Project Aims

- Implement a data pipeline to save API data into a database.
- Run the data pipeline on a daily cronjob.
- Deliver data to the stakeholder by means of a download button on a website.
- For style, the website should be formatted under a darkmode theme.
- Visualise the data on the website through a plotly graph.

### Project Outcomes

- All project aims were met bar the cronjob and backend database. A third party is responsible for this.

### Product Description

The product serves to allow the stakeholder to collect the data automatically in a database, and download the data through a front end website.

### How to run it on your own machine

1. Install the requirements

   ```
   $ pip install -r requirements.txt
   ```

2. Run the app

   ```
   $ streamlit run streamlit_app.py
   ```

