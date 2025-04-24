import streamlit as st
import pandas as pd
#https://medium.com/snowflake/building-a-data-product-catalogue-with-streamlit-in-snowflake-sis-b1e59cf9d7cb
st.set_page_config(layout="wide")

st.title("Data Catalog️")
data_catalogue, data_lineage = st.tabs(["Data Catalogue", "Data Lineage"])
tmp = {'id':1,'name':'EDGAR_2024_GHG','name_readable':'EDGAR Greenhouse Gas Emissions report 2024','describtion':'TODO','products':'TODO'}
df = pd.DataFrame.from_dict([tmp])
st.dataframe(df)