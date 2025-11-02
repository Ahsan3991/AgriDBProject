import psycopg2
import streamlit as st
import pandas as pd
import psycopg2 as pg2
import plotly.express as px
import os
from datetime import datetime, timedelta
from logs import log_operation
from dotenv import load_dotenv

#path to database environment
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../data')
ENV_PATH = os.path.join(DATA_DIR, '.env')
ST_LOGS_PATH = os.path.join(DATA_DIR, 'streamlit_logs.txt')

#connect to database
def get_connection():
    log_operation("Connecting to POSTGRES DB for Dashboard", ST_LOGS_PATH)
    load_dotenv(dotenv_path=ENV_PATH)
    env_url = os.getenv('db_url')
    #print(env_url)
    return psycopg2.connect(env_url)

#fetch all node ids
def fetch_nodes_ids():
    log_operation("Fetching nodes from DB", ST_LOGS_PATH)
    conn = get_connection()
    query = "SELECT node_id FROM esp_nodes ORDER BY node_id"
    df = pd.read_sql(query, conn)
    conn.close()
    return df['node_id'].tolist()

#fetch all node data and crops
def fetch_node_and_crop_data(node_id):
    log_operation("Fetching crops data for respective nodes from DB", ST_LOGS_PATH)
    conn = get_connection()
    query = """
            SELECT n.pseudo_id, n.location, c.crop_name, s.moisture_data, s.temperature_data, s.last_updated 
            FROM ((esp_nodes AS n 
            INNER JOIN crops AS c ON n.pseudo_id = c.pseudo_id) 
            INNER JOIN sensors AS s ON n.pseudo_id = s.pseudo_id)
            WHERE n.node_id = %s
            ORDER BY s.last_updated;
        """
    df = pd.read_sql(query, conn, params=(node_id,))
    conn.close()
    return df

log_operation("Generating streamlit UI and Dashboard", ST_LOGS_PATH)

# ---- STREAMLIT UI ----
st.set_page_config(page_title="AGRI DASHBOARD", layout="wide")
st.title("🌱 AGRI DASHBOARD")
#centering the title
title_alignment="""
<style>
#agri-dashboard {
  text-align: center
}
</style>
"""
st.markdown(title_alignment, unsafe_allow_html=True)

#sidebar: search for node ids
all_nodes = fetch_nodes_ids()
search_input = st.sidebar.text_input("Search Node ID:")

#filter node ids dynamically
if search_input:
    node_options = [n for n in all_nodes if search_input.lower() in str(n).lower()]
else:
    node_options = all_nodes

if not node_options:
    st.sidebar.warning("No nodes found")
    selected_node = None
else:
    selected_node = st.sidebar.selectbox("Select node ID:", node_options)

#display data for the selected node
if selected_node:
    node_df = fetch_node_and_crop_data(selected_node)

    if node_df.empty:
        st.warning("No data found for selected node")
    else:
        pseudo_id = node_df['pseudo_id'].iloc[0]
        location = node_df['location'].iloc[0]
        crop_names = ", ".join(node_df['crop_name'].unique())

        #display pseudo_id and connected crops
        st.markdown(f"### NODE INFO")
        st.info(f"**Pseudo ID -->** {pseudo_id}")
        st.info(f"**Location For Nodes-->** {location}")
        st.info(f"**Planted Crops -->** {crop_names}")

        #date range selection
        min_date = node_df['last_updated'].min()
        max_date = node_df['last_updated'].max()
        start_date,end_date = st.date_input(
            "Selected date range", [min_date, max_date], min_value=min_date, max_value=max_date
        )

        #filter by date range
        filtered_df = node_df[(node_df['last_updated'].dt.date >= start_date) & (node_df['last_updated'].dt.date <= end_date)]

        #plot the line chart
        if not filtered_df.empty:
            fig = px.line(
                filtered_df,
                x = "last_updated",
                y = ["moisture_data", "temperature_data"],
                labels = {"value":"Sensor Value", "last_updated":"Time", "variable": "Sensor"},
                title = f"Sensor data for Node = {pseudo_id}"
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning("No sensor data found for selected node and date range")




