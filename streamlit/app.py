import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# CONEXION TO POSTGRES DB
engine = create_engine(
    "postgresql+psycopg2://food_user:food_password123@postgres-food:5432/food_platform"
)

# CONFIG
st.set_page_config(
    page_title="Food Products Analytics",
    layout="wide"
)

# TITLE 
st.title("🍎 Food Products Data Explorer")
st.write("Interactive exploration of products, nutrition and countries")


# LOAD DATA FROM DBT MART
@st.cache_data
def load_data():
    query = """
        SELECT *
        FROM marts.fact_products"""
    
    df = pd.read_sql(query, engine)
    return df

df_products = load_data()

# SHOW DATA
st.title("Food Products Data")

st.dataframe(df_products)

# UNIFY PRODUCTS AND COUNTRIES TABLES
st.title("Products by Country")
def load_products_countries():
    query = """
        WITH bridge_products_countries AS (
            SELECT 
                b.code AS code_bridge,
                c.country_name
            FROM marts.bridge_product_country b
            JOIN marts.dim_country c ON b.country_name = c.country_name
        )
        
        SELECT
            p.*,
            d.*
        FROM marts.fact_products p
        JOIN bridge_products_countries d ON p.code = d.code_bridge"""
    
    df = pd.read_sql(query, engine)

    return df

df_products_countries = load_products_countries()

# SHOW UNIFIED DATA
st.dataframe(df_products_countries)