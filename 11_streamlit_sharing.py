import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from sqlalchemy import create_engine
import base64
from datetime import date

import toml
#data_toml = toml.load("config.toml")
range_forecast = st.secrets['range_forecast']
greater_than_trend = st.secrets['greater_than_trend']
greater_than_feracast = st.secrets['greater_than_feracast']
site_url = st.secrets['site_url']
n_keywords = st.secrets['n_keywords']

host = st.secrets['host_general']
database = st.secrets['database_general']
user = st.secrets['user_general']
password = st.secrets['password_general']
port= st.secrets['port_general']
postgre_complete_url = st.secrets['postgre_complete_url_general']

host_table = st.secrets['host_details']
database_table = st.secrets['database_details']
user_table = st.secrets['user_details']
password_table = st.secrets['password_details']
port_table = st.secrets['port_details']
postgre_complete_url_table = st.secrets['postgre_complete_url_details']


def _max_width_():
    max_width_str = f"max-width: 1500px;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>    
    """,
        unsafe_allow_html=True, 
    )
_max_width_()

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            .viewerBadge_container__1QSob {display: none !important;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

#Intestazione Pagina
#
#
st.title('Predict Dashboard')

#H1
st.write('Previsione dei trend')

# creazione df geerale
engine = create_engine(postgre_complete_url)
df_general = pd.read_sql_query(f"SELECT * FROM wtforecastgeneral WHERE site = '{site_url}';",con=engine)

#df trend
df_trend = df_general.copy()
df_to_graph = None
for index, row in df_trend.iterrows():
    gt_dates_trend = df_trend['gt_dates']
    df_dit = pd.DataFrame.from_dict(row['gt_dates'], orient='columns')
    df_dit['index'] = pd.to_datetime(df_dit['index'], unit='ms')
    df_dit = df_dit.T
    new_header = df_dit.iloc[0]
    df_dit = df_dit[1:]
    df_dit.columns = new_header
    if df_to_graph is not None:
        df_to_graph = pd.concat([df_to_graph, df_dit], ignore_index=True)
    else:
        df_to_graph = df_dit.copy()
total_trend = df_to_graph.sum()
total_trend = total_trend.to_frame()
total_trend.reset_index(inplace=True)
total_trend = total_trend.rename(columns={'index': 'date',0: 'trend' })
total_trend['date'] = total_trend['date'].astype('datetime64[ns]')
total_trend['trend'] = total_trend['trend'].astype('float')
#total_trend
chart_trend = alt.Chart(total_trend).mark_line().encode(
    x=alt.X('date'),
    y=alt.Y('trend')
).properties(title="Google Trend")
gtrend_regline_chart = chart_trend + chart_trend.transform_regression('date', 'trend').mark_line()
#st.altair_chart(gtrend_regline_chart, use_container_width=True)

# df predict
df_predict = df_general.copy()
df_to_graph = None
for index, row in df_predict.iterrows():
    gt_dates_predict = df_predict['forecast_dates']
    df_dit = pd.DataFrame.from_dict(row['forecast_dates'], orient='columns')
    df_dit['index'] = pd.to_datetime(df_dit['index'], unit='ms')
    df_dit = df_dit.T
    new_header = df_dit.iloc[0]
    df_dit = df_dit[1:]
    df_dit.columns = new_header
    if df_to_graph is not None:
        df_to_graph = pd.concat([df_to_graph, df_dit], ignore_index=True)
    else:
        df_to_graph = df_dit.copy()
total_predict = df_to_graph.sum()
total_predict = total_predict.to_frame()
total_predict.reset_index(inplace=True)
total_predict = total_predict.rename(columns={'index': 'date',0: 'predict' })
total_predict['date'] = total_predict['date'].astype('datetime64[ns]')
total_predict['predict'] = total_predict['predict'].astype('float')
#total_predict

chart_predict = alt.Chart(total_predict).mark_line().encode(
    x=alt.X('date'),
    y=alt.Y('predict')
).properties(title="Trend Forecast")
gtrend_regline_chart = chart_predict + chart_predict.transform_regression('date', 'predict').mark_line()
#st.altair_chart(gtrend_regline_chart, use_container_width=True)

# Full Chart
full_df = pd.merge(total_predict, total_trend, left_on='date', right_on='date', how='left')
#full_df
a = alt.Chart(full_df).mark_area(opacity=0.6, color='#25f4ee').encode(x='date', y='trend')
b = alt.Chart(full_df).mark_area(opacity=1, color='#fe2c55').encode(x='date', y='predict')
c = alt.layer(b, a).properties(title="Forecast and Trend Comparison")
st.altair_chart(c, use_container_width=True)

st.write('---------------------------------------------------')

#
#
# Tabelle generali

# creazione df geerale
engine_table = create_engine(postgre_complete_url_table)
df_general_table = pd.read_sql_query(f"SELECT * FROM wtforecastdetails WHERE site = '{site_url}' AND type_gt_or_forecast = 'type_gt';",con=engine_table)

st.write('Trend Attuali')
df_last_trend = df_general_table.copy()
df_last_trend = df_last_trend[['keyword','ga_search_volume','ga_competition','gsc_avg_pos','gsc_sum_imp','gsc_sum_clic','last_trend','gsc_page']].copy()
#df_last_trend
st.dataframe(data=df_last_trend, width=1500, height=768)


def get_table_download_link_csv(df_last_trend):
    csv = df_last_trend.to_csv(sep=';', decimal=',', index=False).encode('UTF-8')
    b64 = base64.b64encode(csv).decode()
    today = date.today()
    d4 = today.strftime("%b-%d-%Y")
    href = f'<a href="data:file/csv;base64,{b64}" download="WTF_keywords-{d4}.csv" target="_blank">Download</a>'
    return href
st.markdown(get_table_download_link_csv(df_last_trend), unsafe_allow_html=True)
st.write('---------------------------------------------------')


engine_table = create_engine(postgre_complete_url_table)
df_general_table = pd.read_sql_query(f"SELECT * FROM wtforecastdetails WHERE site = '{site_url}' AND type_gt_or_forecast = 'type_forecast';",con=engine_table)

st.write('Trend Futuri')
df_last_forecast = df_general_table.copy()
df_last_forecast = df_last_forecast[['keyword','ga_search_volume','ga_competition','gsc_avg_pos','gsc_sum_imp','gsc_sum_clic','last_forecast','gsc_page']].copy()
#df_last_trend
st.dataframe(data=df_last_forecast, width=1500, height=768)

def get_table_download_link_csv(df_last_forecast):
    csv = df_last_forecast.to_csv(sep=';', decimal=',', index=False).encode('UTF-8')
    b64 = base64.b64encode(csv).decode()
    today = date.today()
    d4 = today.strftime("%b-%d-%Y")
    href = f'<a href="data:file/csv;base64,{b64}" download="WTF_keywords-{d4}.csv" target="_blank">Download</a>'
    return href
st.markdown(get_table_download_link_csv(df_last_forecast), unsafe_allow_html=True)



