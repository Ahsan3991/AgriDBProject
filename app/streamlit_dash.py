import psycopg2
import streamlit as st
import pandas as pd
#import psycopg2 as pg2
import plotly.express as px
import plotly.graph_objects as go
import os
#from datetime import datetime, timedelta
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

#fetch all crops in database
def fetch_crop_data():
    log_operation("Fetching crops data", ST_LOGS_PATH)
    conn = get_connection()
    query = "SELECT c.crop_name FROM crops AS c;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df['crop_name'].unique()



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



# ---- STREAMLIT PAGES FUNCTIONS ----

def dashboard():

    # sidebar: search for node ids
    all_nodes = fetch_nodes_ids()
    search_input = st.sidebar.selectbox("Search Node ID:", all_nodes)

    #display data for the selected node
    if search_input:
        node_df = fetch_node_and_crop_data(search_input)

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
            print(filtered_df)

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


def add_customer():
    st.subheader("Add Customer into Database 👇")
    # centering the title
    sub_title_alignment = """
    <style>
    #add-customer-into-database {
      text-align: center
      
    }
    </style>
    """
    st.markdown(sub_title_alignment, unsafe_allow_html=True)

    # hover color for the submit button
    st.markdown( """
        <style>
        .stFormSubmitButton > button {
          background-color: #0e1117;  /*Initial background color*/
          color: white;             /*Initial text color*/
        }
        .stFormSubmitButton > button:hover {
          background-color: #FF4500;  /*Hover background color*/
          color: white;             /*Hover text color*/
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.form("Customer_add_form", clear_on_submit=True):
        customer_name = st.text_input("Enter Customer Name")
        customer_email= st.text_input("Enter Customer Contact (Email Address)")
        customer_location = st.text_input("Enter Customer Location")
        df = fetch_nodes_ids()
        customer_node_ids = st.multiselect("Select Customer Node IDs:", df)
        #customer_node_ids_input = st.text_input("Enter Node IDs (comma separated)", placeholder="e.g: 101, 102")
        #customer_node_ids = [node.strip() for node in customer_node_ids_input.split(",") if node.strip()]
        df = fetch_crop_data()
        customer_crop_names = st.multiselect("Select Crop Names", df)

        st.write(f"""Click Submit to add following information to Database:
                    \nCustomer Name: {customer_name}
                    \nCustomer Email: {customer_email}
                    \nCustomer Location: {customer_location}
                    \nCustomer Node IDs: {customer_node_ids}
                    \n Custom Crop Names: {customer_crop_names}""")

        submitted = st.form_submit_button("Submit Data", type= "primary", width="stretch")

input_action = st.sidebar.selectbox("Select Action:", ['Dashboard', 'Add Customer', 'Edit Customer Info.'])

if input_action == 'Dashboard':
    dashboard()
if input_action == 'Add Customer':
    add_customer()






