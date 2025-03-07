# ===== IMPORTANT: This must be the first Streamlit command =====
import streamlit as st

# Set page configuration at the very top of the script
st.set_page_config(
    page_title="RealtyLens - Property Analytics",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Now import other libraries
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import datetime
import time
import folium
from streamlit_folium import folium_static, st_folium
import snowflake.connector
import pickle
import os
from pathlib import Path
import hashlib
import shapely.wkt
from shapely.geometry import mapping
import re
import altair as alt
import math
import colorsys
import matplotlib.pyplot as plt
from folium.plugins import MarkerCluster
import urllib.parse
import json
from folium import plugins
import streamlit.components.v1 as components  # Rename to avoid conflict
from folium import MacroElement
from jinja2 import Template

# ======= INITIALIZE SESSION STATE FIRST =======
if 'snowflake_queries' not in st.session_state:
    st.session_state['snowflake_queries'] = 0
if 'cache_hits' not in st.session_state:
    st.session_state['cache_hits'] = 0
if 'last_query_time' not in st.session_state:
    st.session_state['last_query_time'] = None
if 'selected_property' not in st.session_state:
    st.session_state['selected_property'] = None
if 'db_hit_indicator' not in st.session_state:
    st.session_state['db_hit_indicator'] = False
if 'db_hit_timestamp' not in st.session_state:
    st.session_state['db_hit_timestamp'] = None

# ======= PERFORMANCE CONFIGURATION =======
MAX_VISIBLE_MARKERS = 5000  # Increased from 1000 to 5000
ENABLE_DATA_SAMPLING = False  # Disabled sampling for full data display
CACHE_EXPIRATION_DAYS = 30   # Longer cache for better performance

# Create cache directory if it doesn't exist
CACHE_DIR = Path(".streamlit/data_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ======= DATABASE HIT INDICATOR =======
def flash_db_hit_indicator():
    """Activate the database hit indicator"""
    st.session_state['db_hit_indicator'] = True
    st.session_state['db_hit_timestamp'] = datetime.datetime.now()

def render_db_indicator():
    """Display a small indicator showing database connection status"""
    try:
        if "snowflake" in st.secrets:
            # Test connection
            conn = get_snowflake_connection()
            if conn:
                conn.close()
                st.sidebar.success("📊 Database connected", icon="✅")
            else:
                st.sidebar.error("📊 Database disconnected", icon="❌")
        else:
            st.sidebar.warning("📊 Database credentials not found", icon="⚠️")
    except:
        st.sidebar.error("📊 Database error", icon="❌")

# ======= CACHING SYSTEM =======
def get_cache_key(query):
    return hashlib.md5(query.encode()).hexdigest()

def get_cached_data(query):
    cache_key = get_cache_key(query)
    cache_file = CACHE_DIR / f"{cache_key}.pkl"
    
    if cache_file.exists():
        mod_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.datetime.now() - mod_time < datetime.timedelta(days=CACHE_EXPIRATION_DAYS):
            try:
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                    st.session_state['cache_hits'] += 1
                    return data
            except Exception:
                pass
    return None

def save_to_cache(query, data):
    cache_key = get_cache_key(query)
    cache_file = CACHE_DIR / f"{cache_key}.pkl"
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        pass

# ======= SNOWFLAKE CONNECTION =======
def get_snowflake_connection():
    """Create a connection to Snowflake using secrets"""
    try:
        # Check if we're running locally or in Streamlit Cloud
        if "snowflake" in st.secrets:
            # Get snowflake secrets dictionary
            snowflake_secrets = st.secrets.get("snowflake", {})
            
            # Use .get() method for all parameters to avoid KeyError exceptions
            account = snowflake_secrets.get("account")
            user = snowflake_secrets.get("user")
            password = snowflake_secrets.get("password")
            
            # Check if required parameters are present
            if not all([account, user, password]):
                missing = []
                if not account: missing.append("account")
                if not user: missing.append("user")
                if not password: missing.append("password")
                st.error(f"Missing required Snowflake credentials: {', '.join(missing)}")
                return None
            
            # Build minimal connection parameters without any optional parameters
            conn_params = {
                "account": account,
                "user": user,
                "password": password
            }
            
            # Only add database and schema if they exist
            database = snowflake_secrets.get("database")
            if database:
                conn_params["database"] = database
                
            schema = snowflake_secrets.get("schema")
            if schema:
                conn_params["schema"] = schema
            
            # Explicitly avoid using role or warehouse unless specifically provided
            # We won't even check for these parameters
            
            # Connect with only the necessary parameters
            conn = snowflake.connector.connect(**conn_params)
            return conn
        else:
            # For local development - fallback to hardcoded values
            # (You should remove this in production)
            st.warning("Snowflake credentials not found in secrets!")
            return None
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Updated query function to use the connection from secrets
def query_snowflake(query):
    """Execute a query against Snowflake and return results as a DataFrame"""
    conn = get_snowflake_connection()
    
    if conn is None:
        st.error("Could not connect to Snowflake")
        return pd.DataFrame()
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names from cursor description
        columns = [col[0] for col in cursor.description]
        
        # Fetch all results
        data = cursor.fetchall()
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=columns)
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

# Define a global safeguard for any append operations in the app
def safe_append(session_key, value):
    """Safely append to session state lists with type checking"""
    if session_key not in st.session_state:
        st.session_state[session_key] = []
        
    # Check and fix the type if it's not a list
    if not isinstance(st.session_state[session_key], list):
        # Reset to empty list if not already a list
        st.session_state[session_key] = []
    
    # Now safely append
    try:
        st.session_state[session_key].append(value)
    except Exception as e:
        # If append fails, reset and create new list with just this item
        st.session_state[session_key] = [value]

def fix_all_session_state():
    """Fix all session state variables to ensure they have the correct types"""
    # List of all session state keys that should be lists
    list_keys = ['snowflake_queries', 'search_history', 'property_history', 'viewed_properties']
    
    # Fix any list variables
    for key in list_keys:
        if key in st.session_state:
            if not isinstance(st.session_state[key], list):
                st.session_state[key] = []
    
    # Fix individual variables
    scalar_vars = {
        'query_count': 0,
        'db_hit_indicator': False,
        'selected_property': None,
        'map_center': None,
        'current_zip': None,
        'listing_type': 'sale'
    }
    
    for key, default in scalar_vars.items():
        if key not in st.session_state:
            st.session_state[key] = default

# ======= DATA LOADING =======
@st.cache_data
def load_property_data(table_name, limit=None):
    """Load property data with adaptability for different table structures"""
    try:
        # First, check if the table has LOAD_DATE and PROPERTY_SK
        col_query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'JMUSNI07'
        AND TABLE_NAME = '{table_name}'
        """
        
        col_result = query_snowflake(col_query)
        if col_result.empty:
            st.error(f"Could not retrieve column information for {table_name}")
            return create_sample_data_for_table(table_name)
            
        # Extract column names
        columns = col_result['COLUMN_NAME'].tolist()
        
        # Check for required columns
        has_load_date = 'LOAD_DATE' in columns
        has_property_sk = 'PROPERTY_SK' in columns
        
        # Determine price column based on table and available columns
        if "FCT_RENT_LISTING" in table_name and 'RENT_PRICE' in columns:
            price_col = "r.RENT_PRICE AS PRICE"
        elif "FCT_SALE_LISTING" in table_name and 'SALE_PRICE' in columns:
            price_col = "r.SALE_PRICE AS PRICE"
        else:
            # Try to find any price column
            price_candidates = ['PRICE', 'LISTING_PRICE', 'ASKING_PRICE']
            price_col = None
            for candidate in price_candidates:
                if candidate in columns:
                    price_col = f"r.{candidate} AS PRICE"
                    break
            
            if not price_col:
                st.warning(f"No price column found in {table_name}")
                price_col = "NULL AS PRICE"
        
        # Construct the query based on available columns
        if has_load_date and has_property_sk:
            # Use the optimal query with most recent LOAD_DATE
            query = f"""
            WITH latest_load AS (
                SELECT MAX(LOAD_DATE) AS max_load_date 
                FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name}
            )
            SELECT 
                r.LISTING_ID,
                {price_col},
                p.PROPERTY_TYPE,
                p.BEDROOMS,
                p.BATHROOMS,
                p.SQUARE_FOOTAGE,
                p.LATITUDE,
                p.LONGITUDE,
                p.FORMATTED_ADDRESS,
                p.ADDRESS_LINE_1,
                p.ADDRESS_LINE_2,
                p.CITY,
                p.STATE,
                p.ZIP_CODE,
                p.COUNTY,
                p.YEAR_BUILT,
                p.LOT_SIZE,
                p.ZONING_ID,
                p.ZONING_CODE,
                p.ZONING_GROUP,
                p.ZONING_LONG_CODE,
                ST_ASGEOJSON(z.POLYGON_COORDINATES) AS POLYGON_GEOJSON,
                r.DAYS_ON_MARKET,
                r.PROPERTY_STATUS,
                r.STATUS
            """
            
            # Add prediction columns if this is a rental listing
            if "FCT_RENT_LISTING" in table_name:
                query += """,
                pred.PREDICTED_RENT_PRICE,
                pred.RENT_TO_PRICE_RATIO,
                pred.SALE_PRICE
                """
            elif "FCT_SALE_LISTING" in table_name:
                # For sale listings, we need to join to the prediction table using property_sk
                query += """,
                pred.PREDICTED_RENT_PRICE,
                pred.RENT_TO_PRICE_RATIO
                """
            
            query += f"""
            FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name} r
            JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_PROPERTY p 
                ON r.PROPERTY_SK = p.PROPERTY_SK
            LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_ZONING z
                ON p.ZONING_ID = z.ZONING_ID
            """
            
            # Add join to predicted rent prices
            if "FCT_RENT_LISTING" in table_name:
                query += """
                LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.PREDICTED_RENT_PRICES pred
                    ON r.LISTING_ID = pred.LISTING_ID
                """
            elif "FCT_SALE_LISTING" in table_name:
                query += """
                LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.PREDICTED_RENT_PRICES pred
                    ON r.LISTING_ID = pred.LISTING_ID
                """
            
            query += f"""
            WHERE r.LOAD_DATE = (SELECT max_load_date FROM latest_load)
            """
        elif has_property_sk:
            # No LOAD_DATE, but we can still join to DIM_PROPERTY
            query = f"""
            SELECT 
                r.LISTING_ID,
                {price_col},
                p.PROPERTY_TYPE,
                p.BEDROOMS,
                p.BATHROOMS,
                p.SQUARE_FOOTAGE,
                p.LATITUDE,
                p.LONGITUDE,
                p.FORMATTED_ADDRESS,
                p.ADDRESS_LINE_1,
                p.CITY,
                p.STATE,
                p.ZIP_CODE,
                p.COUNTY,
                p.YEAR_BUILT,
                p.LOT_SIZE,
                p.ZONING_ID,
                p.ZONING_CODE,
                p.ZONING_GROUP,
                p.ZONING_LONG_CODE,
                ST_ASGEOJSON(z.POLYGON_COORDINATES) AS POLYGON_GEOJSON,
                r.DAYS_ON_MARKET,
                r.PROPERTY_STATUS,
                r.STATUS
            """
            
            # Add prediction columns if this is a rental listing
            if "FCT_RENT_LISTING" in table_name:
                query += """,
                pred.PREDICTED_RENT_PRICE,
                pred.RENT_TO_PRICE_RATIO,
                pred.SALE_PRICE
                """
            
            query += f"""
            FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name} r
            JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_PROPERTY p 
                ON r.PROPERTY_SK = p.PROPERTY_SK
            LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_ZONING z
                ON p.ZONING_ID = z.ZONING_ID
            """
            
            # Add join to predicted rent prices if this is a rental listing
            if "FCT_RENT_LISTING" in table_name:
                query += """
                LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.PREDICTED_RENT_PRICES pred
                    ON r.LISTING_ID = pred.LISTING_ID
                """
            
            query += f"""
            LIMIT {limit}
            """
        else:
            # Can't join to DIM_PROPERTY, just get whatever we can from the table
            st.warning(f"Table {table_name} cannot be joined to DIM_PROPERTY")
            
            # Create a comma-separated list of columns to select
            available_cols = []
            for col in columns:
                available_cols.append(f"r.{col}")
            
            col_list = ", ".join(available_cols)
            
            query = f"""
            SELECT {col_list}
            FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name} r
            """
        
        # Execute the query safely
        data = query_snowflake(query)
        
        # If no data returned, use sample data
        if data.empty:
            st.warning(f"No data returned from {table_name}. Using sample data.")
            return create_sample_data_for_table(table_name)
        
        # Process the data
        if not data.empty:
            # Handle numeric columns
            numeric_cols = ['BEDROOMS', 'BATHROOMS', 'YEAR_BUILT', 'DAYS_ON_MARKET', 
                          'PRICE', 'SQUARE_FOOTAGE', 'LOT_SIZE', 'LAST_SALE_PRICE', 
                          'LATITUDE', 'LONGITUDE', 'PREDICTED_RENT_PRICE', 'RENT_TO_PRICE_RATIO', 'SALE_PRICE']
            
            for col in numeric_cols:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
            
            return data
        
    except Exception as e:
        st.error(f"Error loading property data: {str(e)}")
        return create_sample_data_for_table(table_name)

# Helper function to create appropriate sample data
def create_sample_data_for_table(table_name):
    """Create sample data based on the table name"""
    if "RENT" in table_name.upper():
        return create_sample_rental_data()
    else:
        return create_sample_sales_data()

# ======= HELPER FUNCTIONS FOR SAFE ACCESS =======
def safe_get(obj, key, default=""):
    """Safely get a value from a dictionary or pandas Series"""
    if isinstance(obj, dict):
        return obj.get(key, default)
    elif isinstance(obj, pd.Series):
        return obj.get(key, default) if key in obj.index else default
    return default

def format_price(price, currency="$"):
    """Format price with commas and currency symbol"""
    if pd.isna(price):
        return "N/A"
    try:
        return f"{currency}{int(float(price)):,}"
    except:
        return f"{currency}{price}"

def format_address(property_data):
    """Format the address with safety checks for list operations"""
    # Initialize as an empty list
    address_parts = []
    
    # Build the address parts safely
    for field in ['ADDRESS_LINE_1', 'CITY', 'STATE', 'ZIP_CODE']:
        if field in property_data and pd.notna(property_data[field]):
            # Use safe append
            address_parts = safe_append(address_parts, str(property_data[field]))
    
    # Join the parts
    return ", ".join(address_parts)

# ======= DISPLAY PROPERTY DETAILS ======
def display_property_details(property_data):
    """Display detailed information about a selected property"""
    if not property_data:
        st.warning("No property details available")
        return
    
    try:
        # Create columns for key property information
        col1, col2 = st.columns(2)
        
        # Basic property information (left column)
        with col1:
            address = property_data.get('FORMATTED_ADDRESS', 'No address available')
            st.markdown(f"### {address}")
            
            # Property status
            status = property_data.get('PROPERTY_STATUS', 'Unknown')
            if status:
                if status.lower() == 'active':
                    st.markdown(f"**Status:** 🟢 {status}")
                elif status.lower() == 'pending':
                    st.markdown(f"**Status:** 🟠 {status}")
                elif status.lower() == 'sold':
                    st.markdown(f"**Status:** 🔴 {status}")
                else:
                    st.markdown(f"**Status:** {status}")
            
            # Basic property details
            beds = int(property_data.get('BEDROOMS', 0)) if pd.notna(property_data.get('BEDROOMS')) else 0
            baths = property_data.get('BATHROOMS', 0)
            sqft = int(property_data.get('SQUARE_FOOTAGE', 0)) if pd.notna(property_data.get('SQUARE_FOOTAGE')) else 0
            
            st.markdown(f"**{beds} bed, {baths} bath, {sqft:,} sq ft**")
            
            # Property type
            prop_type = property_data.get('PROPERTY_TYPE', 'Unknown')
            if prop_type:
                st.markdown(f"**Type:** {prop_type}")
            
            # Year built
            year_built = int(property_data.get('YEAR_BUILT', 0)) if pd.notna(property_data.get('YEAR_BUILT')) else 'Unknown'
            if year_built != 'Unknown':
                st.markdown(f"**Year Built:** {year_built}")
            
            # Days on market
            days = property_data.get('DAYS_ON_MARKET')
            if days and pd.notna(days):
                st.markdown(f"**Days on Market:** {int(days)}")
            
            # Lot size if available
            lot_size = property_data.get('LOT_SIZE')
            if lot_size and pd.notna(lot_size):
                st.markdown(f"**Lot Size:** {int(lot_size):,} sq ft")
        
        # Price information (right column)
        with col2:
            price = property_data.get('PRICE', 0)
            if price and pd.notna(price):
                st.markdown(f"## ${price:,.0f}")
                
                # Calculate price per square foot
                if sqft > 0:
                    price_per_sqft = price / sqft
                    st.markdown(f"**${price_per_sqft:.2f}/sq ft**")
            
            # Zoning information if available
            zoning_code = property_data.get('ZONING_CODE')
            zoning_group = property_data.get('ZONING_GROUP')
            
            if zoning_code and pd.notna(zoning_code):
                st.markdown("#### Zoning Information")
                st.markdown(f"**Code:** {zoning_code}")
                if zoning_group and pd.notna(zoning_group):
                    st.markdown(f"**Type:** {zoning_group}")
            
            # Investment metrics (if available)
            pred_rent = property_data.get('PREDICTED_RENT_PRICE')
            if pred_rent and pd.notna(pred_rent):
                st.markdown("#### Investment Analysis")
                st.markdown(f"**Est. Monthly Rent:** ${pred_rent:,.0f}")
                
                # Calculate ROI
                rent_to_price = property_data.get('RENT_TO_PRICE_RATIO')
                if rent_to_price and pd.notna(rent_to_price):
                    annual_yield = rent_to_price * 12 * 100
                    st.markdown(f"**Annual Yield:** {annual_yield:.2f}%")
                    
                    # Calculate mortgage payment (estimated)
                    mortgage_payment = (price * 0.8 * (0.05/12) * (1 + 0.05/12)**(30*12)) / ((1 + 0.05/12)**(30*12) - 1)
                    st.markdown(f"**Est. Mortgage:** ${mortgage_payment:,.0f}/mo")
                    
                    # Calculate cash flow
                    monthly_expenses = price * 0.02 / 12  # Estimate 2% annual for taxes, insurance, maintenance
                    cash_flow = pred_rent - mortgage_payment - monthly_expenses
                    
                    if cash_flow > 0:
                        st.markdown(f"**Monthly Cash Flow:** 🟢 +${cash_flow:,.0f}")
                    else:
                        st.markdown(f"**Monthly Cash Flow:** 🔴 -${-cash_flow:,.0f}")
        
        # Create a map showing just this property
        st.markdown("### Property Location")
        
        # Get lat/lon for the property
        lat = property_data.get('LATITUDE')
        lon = property_data.get('LONGITUDE')
        
        if lat and lon and pd.notna(lat) and pd.notna(lon):
            # Create a lightweight map centered on this property
            property_map = folium.Map(
                location=[lat, lon], 
                zoom_start=15, 
                tiles="OpenStreetMap",
                prefer_canvas=True  # Better performance
            )
            
            # Create a marker for this property
            popup_html = f"<strong>{address}</strong><br>${price:,.0f}<br>{beds} bed, {baths} bath, {sqft:,} sq ft"
            
            folium.Marker(
                [lat, lon],
                popup=popup_html,
                tooltip=address,
                icon=folium.Icon(color='red', icon='home', prefix='fa')
            ).add_to(property_map)
            
            # Display the map with optimized loading
            with st.spinner("Loading property location..."):
                folium_static(property_map, width=1000, height=600)
        else:
            st.warning("No location data available for this property")
        
    except Exception as e:
        st.error(f"Error displaying property details: {str(e)}")

class Legend(MacroElement):
    """Custom Legend for Folium maps"""
    
    def __init__(self):
        """Initialize the legend."""
        super(Legend, self).__init__()
        self._name = "Legend"
        self._template = Template("""
            {% macro script(this, kwargs) %}
            var legend = L.control({position: 'bottomright'});
            legend.onAdd = function (map) {
                var div = L.DomUtil.create('div', 'info legend');
                div.innerHTML = `
                <div style="
                    background-color: white; 
                    padding: 10px; 
                    border: 2px solid grey; 
                    border-radius: 5px;
                    font-family: Arial, sans-serif;">
                    <p style="margin: 0; text-align: center;"><strong>Investment Quality</strong></p>
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: green; margin-right: 5px;"></div>
                        <span>Excellent (>10% yield)</span>
                    </div>
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: lightgreen; margin-right: 5px;"></div>
                        <span>Good (8-10% yield)</span>
                    </div>
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: orange; margin-right: 5px;"></div>
                        <span>Average (6-8% yield)</span>
                    </div>
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: red; margin-right: 5px;"></div>
                        <span>Below average (<6% yield)</span>
                    </div>
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: blue; margin-right: 5px;"></div>
                        <span>Unknown yield</span>
                    </div>
                </div>`;
                return div;
            };
            legend.addTo({{ this._parent.get_name() }});
            {% endmacro %}
            """)

def create_property_map(property_data, listing_type="sale"):
    """Create an interactive map with color-coded price tag markers"""
    try:
        if property_data is None or property_data.empty:
            return folium.Map(location=[47.6062, -122.3321], zoom_start=12)
        
        # Drop any rows with missing coordinates
        valid_data = property_data.dropna(subset=['LATITUDE', 'LONGITUDE'])
        
        if valid_data.empty:
            return folium.Map(location=[47.6062, -122.3321], zoom_start=12)
        
        # Calculate map center using median of coordinates
        map_center = [valid_data['LATITUDE'].median(), valid_data['LONGITUDE'].median()]
        
        # Create base map
        property_map = folium.Map(
            location=map_center, 
            zoom_start=12, 
            control_scale=True
        )
        
        # Create a marker cluster group with optimized settings
        marker_cluster = MarkerCluster(
            name="Properties",
            options={
                'maxClusterRadius': 60,
                'disableClusteringAtZoom': 16,
                'chunkedLoading': True,
                'chunkDelay': 10
            }
        )
        marker_cluster.add_to(property_map)
        
        # Process properties in smaller batches for better performance
        batch_size = 100
        total_batches = len(valid_data) // batch_size + (1 if len(valid_data) % batch_size > 0 else 0)
        
        # CSS for popup styling
        popup_style = """
        <style>
            .property-popup {
                font-family: Arial, sans-serif;
                font-size: 12px;
                padding: 5px;
                min-width: 200px;
            }
            .property-popup h3 {
                margin-top: 0;
                margin-bottom: 10px;
                font-size: 14px;
                font-weight: bold;
            }
            .property-popup table {
                width: 100%;
                border-collapse: collapse;
            }
            .property-popup table td {
                padding: 3px 0;
                vertical-align: top;
            }
            .property-popup .links {
                margin-top: 10px;
                border-top: 1px solid #eee;
                padding-top: 5px;
                text-align: center;
            }
        </style>
        """
        
        # Add properties to map
        for i in range(total_batches):
            batch_start = i * batch_size
            batch_end = min(batch_start + batch_size, len(valid_data))
            
            # Process this batch
            for idx in range(batch_start, batch_end):
                try:
                    prop = valid_data.iloc[idx]
                    
                    # Check if we have lat/long
                    if pd.isna(prop['LATITUDE']) or pd.isna(prop['LONGITUDE']):
                        continue
                    
                    # Get property details
                    lat = float(prop['LATITUDE'])
                    lon = float(prop['LONGITUDE'])
                    
                    # Skip properties with invalid coordinates
                    if abs(lat) > 90 or abs(lon) > 180:
                        continue
                    
                    # Create color based on investment quality for sale properties
                    color = 'blue'  # Default color
                    
                    if listing_type == "sale" and 'RENT_TO_PRICE_RATIO' in prop and pd.notna(prop['RENT_TO_PRICE_RATIO']):
                        annual_yield = prop['RENT_TO_PRICE_RATIO'] * 12 * 100
                        
                        if annual_yield > 10:
                            color = 'green'  # Excellent investment
                        elif annual_yield > 8:
                            color = 'lightgreen'  # Good investment
                        elif annual_yield > 6:
                            color = 'orange'  # Average investment
                        else:
                            color = 'red'  # Below average investment
                    
                    # Get common property details
                    price = prop.get('PRICE', 0)
                    bedrooms = int(prop.get('BEDROOMS', 0)) if pd.notna(prop.get('BEDROOMS', 0)) else 0
                    bathrooms = prop.get('BATHROOMS', 0)
                    
                    # Create the popup HTML
                    popup_html = create_property_popup(prop, popup_style, listing_type, idx)
                    
                    # Add marker to map
                    folium.Marker(
                        [lat, lon],
                        icon=folium.Icon(color=color, icon='home', prefix='fa'),
                        popup=folium.Popup(popup_html, max_width=300),
                        tooltip=f"${price:,.0f} - {bedrooms} bed, {bathrooms} bath"
                    ).add_to(marker_cluster)
                    
                except Exception as e:
                    # Skip any problematic markers
                    continue
        
        return property_map
    
    except Exception as e:
        st.error(f"Error creating map: {str(e)}")
        return folium.Map(location=[47.6062, -122.3321], zoom_start=12)

def create_property_popup(property_row, popup_style, listing_type, idx):
    """Create detailed popup HTML for a property"""
    try:
        # Extract property info
        address = property_row.get('FORMATTED_ADDRESS', 'Address not available')
        price = property_row.get('PRICE', 0)
        bedrooms = property_row.get('BEDROOMS', 0)
        bathrooms = property_row.get('BATHROOMS', 0)
        sqft = property_row.get('SQUARE_FOOTAGE', 0)
        
        # Start popup HTML
        popup_html = f"""
        {popup_style}
        <div class="property-popup">
            <h3>{address}</h3>
            <table>
                <tr>
                    <td><strong>Price:</strong></td>
                    <td>${price:,.0f}</td>
                </tr>
                <tr>
                    <td><strong>Beds/Baths:</strong></td>
                    <td>{bedrooms} bed, {bathrooms} bath</td>
                </tr>
        """
        
        # Add square footage if available
        if sqft and pd.notna(sqft):
            popup_html += f"""
                <tr>
                    <td><strong>Size:</strong></td>
                    <td>{sqft:,.0f} sq ft</td>
                </tr>
            """
        
        # Add property type if available
        if 'PROPERTY_TYPE' in property_row and pd.notna(property_row['PROPERTY_TYPE']):
            popup_html += f"""
                <tr>
                    <td><strong>Type:</strong></td>
                    <td>{property_row['PROPERTY_TYPE']}</td>
                </tr>
            """
        
        # Add zoning group if available
        if 'ZONING_GROUP' in property_row and pd.notna(property_row['ZONING_GROUP']):
            popup_html += f"""
                <tr>
                    <td><strong>Zoning:</strong></td>
                    <td>{property_row['ZONING_GROUP']}</td>
                </tr>
            """
        
        # Add year built if available
        if 'YEAR_BUILT' in property_row and pd.notna(property_row['YEAR_BUILT']):
            popup_html += f"""
                <tr>
                    <td><strong>Year Built:</strong></td>
                    <td>{int(property_row['YEAR_BUILT'])}</td>
                </tr>
            """
        
        # Add investment metrics for sale listings
        if listing_type == "sale" and 'PREDICTED_RENT_PRICE' in property_row and pd.notna(property_row['PREDICTED_RENT_PRICE']):
            pred_rent = property_row['PREDICTED_RENT_PRICE']
            popup_html += f"""
                <tr>
                    <td><strong>Est. Rent:</strong></td>
                    <td>${pred_rent:,.0f}/mo</td>
                </tr>
            """
            
            if 'RENT_TO_PRICE_RATIO' in property_row and pd.notna(property_row['RENT_TO_PRICE_RATIO']):
                annual_yield = property_row['RENT_TO_PRICE_RATIO'] * 12 * 100
                yield_color = "#27ae60" if annual_yield > 8 else ("#f39c12" if annual_yield > 6 else "#e74c3c")
                popup_html += f"""
                    <tr>
                        <td><strong>Annual Yield:</strong></td>
                        <td><span style="color:{yield_color}; font-weight:bold;">{annual_yield:.2f}%</span></td>
                    </tr>
                """
        
        # Close the table and add links with Google search instead of Maps
        encoded_address = urllib.parse.quote(address)
        popup_html += f"""
            </table>
            <div class="links">
                <a href="https://www.google.com/search?q={encoded_address}" target="_blank">Google</a> | 
                <a href="https://www.zillow.com/homes/{encoded_address}_rb/" target="_blank">Zillow</a>
            </div>
        </div>
        """
        
        return popup_html
    
    except Exception as e:
        # Return a simple popup on error
        return f"<div>Property at {property_row.get('FORMATTED_ADDRESS', 'Unknown')}</div>"

def apply_filters(property_data):
    """Apply user-selected filters to the property data"""
    if property_data is None or property_data.empty:
        return property_data
    
    filtered_data = property_data.copy()
    
    # Price filter
    min_price = st.sidebar.slider(
        "Min Price", 
        min_value=float(filtered_data['PRICE'].min()),
        max_value=float(filtered_data['PRICE'].max()),
        value=float(filtered_data['PRICE'].min()),
        step=50000.0
    )
    
    max_price = st.sidebar.slider(
        "Max Price", 
        min_value=float(filtered_data['PRICE'].min()),
        max_value=float(filtered_data['PRICE'].max()),
        value=float(filtered_data['PRICE'].max()),
        step=50000.0
    )
    
    filtered_data = filtered_data[(filtered_data['PRICE'] >= min_price) & 
                                 (filtered_data['PRICE'] <= max_price)]
    
    # Bedrooms filter
    if 'BEDROOMS' in filtered_data.columns:
        # Filter out None values before sorting
        valid_bedrooms = [b for b in filtered_data['BEDROOMS'].unique() if pd.notna(b)]
        bedrooms = st.sidebar.multiselect(
            "Bedrooms",
            options=sorted(valid_bedrooms),
            default=None
        )
        
        if bedrooms:
            filtered_data = filtered_data[filtered_data['BEDROOMS'].isin(bedrooms)]
    
    # Bathrooms filter
    if 'BATHROOMS' in filtered_data.columns:
        # Filter out None values before sorting
        valid_bathrooms = [b for b in filtered_data['BATHROOMS'].unique() if pd.notna(b)]
        
        bathrooms = st.sidebar.multiselect(
            "Bathrooms",
            options=sorted(valid_bathrooms),
            default=None
        )
        
        if bathrooms:
            filtered_data = filtered_data[filtered_data['BATHROOMS'].isin(bathrooms)]
    
    # Property type filter
    if 'PROPERTY_TYPE' in filtered_data.columns:
        # Filter out None values before sorting
        valid_property_types = [pt for pt in filtered_data['PROPERTY_TYPE'].unique() if pd.notna(pt)]
        
        property_types = st.sidebar.multiselect(
            "Property Type",
            options=sorted(valid_property_types),
            default=None
        )
        
        if property_types:
            filtered_data = filtered_data[filtered_data['PROPERTY_TYPE'].isin(property_types)]
    
    # Add investment filters only for sale properties
    if 'RENT_TO_PRICE_RATIO' in filtered_data.columns and st.session_state.listing_type == "sale":
        st.sidebar.markdown("### Investment Filters")
        
        # Calculate annual yield percentage for all properties
        filtered_data['ANNUAL_YIELD'] = filtered_data['RENT_TO_PRICE_RATIO'] * 12 * 100
        
        # Get min and max yield values (handling NaN values)
        non_na_yields = filtered_data['ANNUAL_YIELD'].dropna()
        if not non_na_yields.empty:
            min_yield = non_na_yields.min()
            max_yield = non_na_yields.max()
        else:
            min_yield = 0
            max_yield = 15
        
        # Add yield filter slider
        min_yield_filter = st.sidebar.slider(
            "Min Annual Yield (%)", 
            min_value=float(min_yield),
            max_value=float(max_yield),
            value=float(min_yield),
            step=0.5
        )
        
        # Apply investment filter
        filtered_data = filtered_data[
            (filtered_data['ANNUAL_YIELD'] >= min_yield_filter) | 
            (filtered_data['ANNUAL_YIELD'].isna())
        ]
        
        # Investment quality categories
        investment_quality = st.sidebar.multiselect(
            "Investment Quality",
            options=["Excellent (>10%)", "Good (8-10%)", "Average (6-8%)", "Below Average (<6%)"],
            default=None
        )
        
        if investment_quality:
            quality_mask = pd.Series(False, index=filtered_data.index)
            
            for quality in investment_quality:
                if "Excellent" in quality:
                    quality_mask = quality_mask | (filtered_data['ANNUAL_YIELD'] > 10)
                elif "Good" in quality:
                    quality_mask = quality_mask | ((filtered_data['ANNUAL_YIELD'] > 8) & (filtered_data['ANNUAL_YIELD'] <= 10))
                elif "Average" in quality:
                    quality_mask = quality_mask | ((filtered_data['ANNUAL_YIELD'] > 6) & (filtered_data['ANNUAL_YIELD'] <= 8))
                elif "Below Average" in quality:
                    quality_mask = quality_mask | (filtered_data['ANNUAL_YIELD'] <= 6)
            
            # Apply the quality mask, but also keep properties without yield data
            filtered_data = filtered_data[quality_mask | filtered_data['ANNUAL_YIELD'].isna()]
    
    return filtered_data

def display_sale_rent_prediction_metrics(property_data):
    """Display rent prediction metrics for sale properties"""
    if property_data is None or property_data.empty:
        return
    
    # Check if prediction columns exist in the data
    has_predictions = 'PREDICTED_RENT_PRICE' in property_data.columns
    
    if not has_predictions or property_data['PREDICTED_RENT_PRICE'].isna().all():
        return
    
    # Create a container for the predictions
    st.subheader("💼 Investment Opportunity Analysis")
    
    # Create metrics row
    col1, col2, col3 = st.columns(3)
    
    # Calculate metrics
    avg_predicted_rent = property_data['PREDICTED_RENT_PRICE'].mean()
    
    if 'RENT_TO_PRICE_RATIO' in property_data.columns:
        avg_rent_to_price = property_data['RENT_TO_PRICE_RATIO'].mean() * 100 * 12  # Annual percentage
        col1.metric("Avg. Predicted Rent", f"${avg_predicted_rent:,.2f}/mo")
        col2.metric("Avg. Annual Yield", f"{avg_rent_to_price:.2f}%")
        
        # Calculate breakeven time (years to recoup purchase price from rent)
        breakeven_years = 100 / avg_rent_to_price if avg_rent_to_price > 0 else float('inf')
        col3.metric("Breakeven Time", f"{breakeven_years:.1f} years")
    else:
        col1.metric("Avg. Predicted Rent", f"${avg_predicted_rent:,.2f}/mo")
    
    # Display top investment opportunities
    if len(property_data) > 5 and 'RENT_TO_PRICE_RATIO' in property_data.columns:
        st.markdown("### Top Investment Opportunities")
        
        # Get top 5 properties by rent-to-price ratio
        top_investments = property_data.sort_values('RENT_TO_PRICE_RATIO', ascending=False).head(5)
        
        # Create a table of top investments
        investment_table = pd.DataFrame({
            'Address': top_investments['FORMATTED_ADDRESS'],
            'Price': top_investments['PRICE'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"),
            'Predicted Rent': top_investments['PREDICTED_RENT_PRICE'].apply(lambda x: f"${x:,.0f}/mo" if pd.notna(x) else "N/A"),
            'Annual Yield': top_investments['RENT_TO_PRICE_RATIO'].apply(lambda x: f"{x*100*12:.2f}%" if pd.notna(x) else "N/A")
        })
        
        st.dataframe(investment_table, use_container_width=True)
        
        # Create a scatter plot of price vs predicted rent
        st.markdown("### Price vs. Predicted Rent")
        
        fig = px.scatter(
            property_data.dropna(subset=['PREDICTED_RENT_PRICE', 'PRICE']), 
            x='PRICE', 
            y='PREDICTED_RENT_PRICE',
            color='RENT_TO_PRICE_RATIO',
            color_continuous_scale='Viridis',
            hover_name='FORMATTED_ADDRESS',
            labels={
                'PRICE': 'Purchase Price ($)',
                'PREDICTED_RENT_PRICE': 'Predicted Monthly Rent ($)',
                'RENT_TO_PRICE_RATIO': 'Rent-to-Price Ratio'
            },
            title='Relationship Between Purchase Price and Rental Potential'
        )
        
        fig.update_layout(
            height=500,
            coloraxis_colorbar_title='Rent-to-Price Ratio'
        )
        
        st.plotly_chart(fig, use_container_width=True)

def display_property_statistics_main(property_data, listing_type="sale"):
    """Display comprehensive property statistics for the dataset"""
    if property_data is None or property_data.empty:
        st.warning("No data available for statistics")
        return
    
    # Set title based on listing type
    market_type = "Rental Market" if listing_type == "rent" else "Sales Market"
    st.subheader(f"{market_type} Overview")
    
    # Create statistics tabs
    stats_tabs = st.tabs(["Price Stats", "Property", "Market"])
    
    # --------- PRICE STATISTICS TAB ---------
    with stats_tabs[0]:
        cols = st.columns(2)
        
        # Price column name depends on listing type
        price_col = 'PRICE'
        price_label = "Sale Price" if listing_type == "sale" else "Monthly Rent"
        
        # Calculate price statistics
        if price_col in property_data.columns:
            price_data = property_data[price_col].dropna()
            
            if not price_data.empty:
                median_price = price_data.median()
                avg_price = price_data.mean()
                min_price = price_data.min()
                max_price = price_data.max()
                
                # Display price statistics
                with cols[0]:
                    st.metric("Median", f"${median_price:,.0f}")
                    st.caption(f"Range: ${min_price:,.0f} - ${max_price:,.0f}")
                
                with cols[1]:
                    st.metric("Average", f"${avg_price:,.0f}")
                
                # Create a price histogram
                st.markdown("##### Price Distribution")
                
                # Create histogram bins
                num_bins = min(20, len(price_data) // 5) if len(price_data) > 10 else 5
                
                fig = px.histogram(
                    property_data,
                    x=price_col,
                    nbins=num_bins,
                    labels={price_col: price_label, 'count': 'Number of Properties'},
                    title=f'Distribution of {price_label}s'
                )
                
                fig.update_layout(height=250)
                st.plotly_chart(fig, use_container_width=True)
    
    # --------- PROPERTY CHARACTERISTICS TAB ---------
    with stats_tabs[1]:
        property_cols = st.columns(2)
        
        # Bedroom statistics
        with property_cols[0]:
            if 'BEDROOMS' in property_data.columns:
                beds_data = property_data['BEDROOMS'].dropna()
                
                if not beds_data.empty:
                    avg_beds = beds_data.mean()
                    st.metric("Avg. Beds", f"{avg_beds:.1f}")
                    
                    # Bedroom distribution summary
                    bed_counts = beds_data.value_counts().sort_index()
                    bed_summary = " | ".join([f"{b}br: {c}" for b, c in bed_counts.items()])
                    st.caption(bed_summary)
        
        # Property type distribution
        with property_cols[1]:
            if 'PROPERTY_TYPE' in property_data.columns:
                type_data = property_data['PROPERTY_TYPE'].dropna()
                
                if not type_data.empty:
                    # Get the most common type
                    top_type = type_data.value_counts().index[0]
                    top_pct = type_data.value_counts(normalize=True).iloc[0] * 100
                    
                    st.metric("Top Type", f'"{top_type}"')
                    st.caption(f"{top_pct:.0f}%")
        
        # Show property type pie chart
        if 'PROPERTY_TYPE' in property_data.columns:
            type_counts = property_data['PROPERTY_TYPE'].value_counts()
            if len(type_counts) > 0:
                st.markdown("##### Property Types")
                
                # Limit to top 5 types plus "Other" for cleaner display
                if len(type_counts) > 5:
                    top_types = type_counts.head(5)
                    other_count = type_counts[5:].sum()
                    
                    # Create a new series with top 5 + Other
                    plot_types = pd.Series({**top_types.to_dict(), 'Other': other_count})
                else:
                    plot_types = type_counts
                
                fig = px.pie(
                    values=plot_types.values,
                    names=plot_types.index,
                    title='Property Types',
                    hole=0.4
                )
                
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
    
    # --------- MARKET METRICS TAB ---------
    with stats_tabs[2]:
        market_cols = st.columns(2)
        
        # Price per sq ft
        with market_cols[0]:
            if 'PRICE' in property_data.columns and 'SQUARE_FOOTAGE' in property_data.columns:
                # Calculate price per square foot
                property_data['PRICE_PER_SQFT'] = property_data['PRICE'] / property_data['SQUARE_FOOTAGE']
                price_sqft_data = property_data['PRICE_PER_SQFT'].dropna()
                
                if not price_sqft_data.empty:
                    avg_price_sqft = price_sqft_data.median()  # Use median to avoid outliers
                    st.metric("Price/SqFt", f"${avg_price_sqft:.2f}")
        
        # Days on market
        with market_cols[1]:
            if 'DAYS_ON_MARKET' in property_data.columns:
                dom_data = property_data['DAYS_ON_MARKET'].dropna()
                
                if not dom_data.empty:
                    avg_dom = dom_data.mean()
                    st.metric("Days on Mkt", f"{avg_dom:.0f}")
        
        # Days on market histogram
        if 'DAYS_ON_MARKET' in property_data.columns:
            dom_data = property_data['DAYS_ON_MARKET'].dropna()
            
            if len(dom_data) > 5:
                st.markdown("##### Days on Market")
                
                # Limit to 90 days for better visualization
                dom_data = dom_data[dom_data <= 90]
                
                fig = px.histogram(
                    dom_data,
                    nbins=min(20, len(dom_data) // 5),
                    labels={'value': 'Days on Market', 'count': 'Number of Properties'},
                    title='Days on Market Distribution (up to 90 days)'
                )
                
                fig.update_layout(height=250)
                st.plotly_chart(fig, use_container_width=True)

def display_investment_heatmap_legend():
    """Display the investment heat map legend in the Streamlit UI"""
    st.sidebar.markdown("## Investment Heat Map")
    st.sidebar.markdown("Color indicates investment quality based on annual rental yield:")
    
    # Create a consistent style for the legend items
    legend_style = """
    <style>
    .legend-item {
        display: flex;
        align-items: center;
        margin-bottom: 10px;
    }
    .color-box {
        width: 20px;
        height: 20px;
        margin-right: 10px;
        border-radius: 3px;
    }
    .legend-text {
        font-size: 14px;
    }
    </style>
    """
    
    st.sidebar.markdown(legend_style, unsafe_allow_html=True)
    
    # Create legend items
    legend_html = """
    <div class="legend-item">
        <div class="color-box" style="background-color: #27ae60;"></div>
        <div class="legend-text"><strong>Excellent</strong> (>8% yield)</div>
    </div>
    <div class="legend-item">
        <div class="color-box" style="background-color: #f39c12;"></div>
        <div class="legend-text"><strong>Good</strong> (6-8% yield)</div>
    </div>
    <div class="legend-item">
        <div class="color-box" style="background-color: #e74c3c;"></div>
        <div class="legend-text"><strong>Below Average</strong> (<6% yield)</div>
    </div>
    <div class="legend-item">
        <div class="color-box" style="background-color: #3498db;"></div>
        <div class="legend-text"><strong>Not Evaluated</strong></div>
    </div>
    """
    
    st.sidebar.markdown(legend_html, unsafe_allow_html=True)

def main():
    """Main Streamlit application"""
    st.title("🏠 RealtyLens: Real Estate Explorer")
    
    # Fixed table options matching your schema
    table_options = ["FCT_SALE_LISTING", "FCT_RENT_LISTING"]
    
    # Sidebar for filters and options
    st.sidebar.title("Options")
    
    # Table selection - use the correct tables
    selected_table = st.sidebar.selectbox(
        "Select Dataset:",
        options=table_options,
        index=0
    )
    
    # Set listing type based on selected table
    if "FCT_RENT_LISTING" in selected_table:
        st.session_state.listing_type = "rent"
    else:
        st.session_state.listing_type = "sale"
    
    # Show investment heat map legend in sidebar if viewing sales listings
    if st.session_state.listing_type == "sale":
        display_investment_heatmap_legend()
    
    # Progress bar for better UX
    progress_bar = st.progress(0)
    progress_bar.progress(10)
    
    # Load data without any limit
    data = load_property_data(selected_table, limit=None)
    progress_bar.progress(30)
    
    # Check if data is available
    if len(data) > 0:
        # Apply other filters from the sidebar
        filtered_data = apply_filters(data)
        progress_bar.progress(50)
        
        # Store the filtered data in session state for use in other functions
        st.session_state.property_data = filtered_data
        
        # Sample data for better performance if needed
        display_data = filtered_data
        if len(filtered_data) > MAX_VISIBLE_MARKERS and ENABLE_DATA_SAMPLING:
            display_data = filtered_data.sample(MAX_VISIBLE_MARKERS)
            st.info(f"Showing a sample of {MAX_VISIBLE_MARKERS} properties for better performance. Filter further to see more specific results.")
            st.write(f"Total matching properties: {len(filtered_data)}")
        else:
            display_data = filtered_data
            st.write(f"Showing {len(filtered_data)} properties")
        
        progress_bar.progress(70)
        
        # Improved layout for property map
        if filtered_data is not None and not filtered_data.empty:
            # Check URL parameters for property selection
            property_selected = False
            if "property_id" in st.query_params:
                try:
                    property_id = int(st.query_params["property_id"])
                    if property_id >= 0 and property_id < len(filtered_data):
                        st.session_state.selected_property = filtered_data.iloc[property_id].to_dict()
                        property_selected = True
                except:
                    # Clear invalid property_id
                    st.query_params.clear()
            
            # Full-width map for better visibility
            st.subheader("Property Map")
            
            # Get property map (without show_zoning parameter)
            property_map = create_property_map(filtered_data, st.session_state.listing_type)
            
            # Display the map with full width
            folium_static(property_map, width=1000, height=600)
            
            # Show investment metrics below the map if available for sales listings
            if st.session_state.listing_type == "sale" and 'PREDICTED_RENT_PRICE' in filtered_data.columns:
                st.markdown("<hr style='margin-top: 30px; margin-bottom: 30px;'>", unsafe_allow_html=True)
                display_sale_rent_prediction_metrics(filtered_data)
            
            # If a property is selected, show its details
            if 'selected_property' in st.session_state and st.session_state.selected_property:
                st.markdown("<hr style='margin-top: 30px; margin-bottom: 30px;'>", unsafe_allow_html=True)
                st.subheader("Property Details")
                display_property_details(st.session_state.selected_property)
                
                # Add a button to clear selection
                if st.button("Close Property Details"):
                    st.session_state.selected_property = None
                    # Clear URL parameters
                    st.query_params.clear()
                    st.experimental_rerun()
            
            # Show property highlights
            if not property_selected:
                st.markdown("<hr style='margin-top: 30px; margin-bottom: 30px;'>", unsafe_allow_html=True)
                st.subheader("Property Highlights")
                
                # Create columns for property highlights
                col1, col2 = st.columns(2)
                
                with col1:
                    # Most expensive property
                    if 'PRICE' in filtered_data.columns and not filtered_data.empty:
                        expensive = filtered_data.loc[filtered_data['PRICE'].idxmax()]
                        st.markdown("#### Most Expensive Property")
                        st.markdown(f"**${expensive['PRICE']:,.0f}** - {expensive['FORMATTED_ADDRESS']}")
                        st.markdown(f"{int(expensive['BEDROOMS'])} bed, {expensive['BATHROOMS']} bath, {int(expensive['SQUARE_FOOTAGE']):,} sq ft")
                
                with col2:
                    # Best investment property (if applicable) - only for sale listings
                    if st.session_state.listing_type == "sale" and 'RENT_TO_PRICE_RATIO' in filtered_data.columns and not filtered_data.empty and not filtered_data['RENT_TO_PRICE_RATIO'].isna().all():
                        best_investment = filtered_data.loc[filtered_data['RENT_TO_PRICE_RATIO'].idxmax()]
                        annual_yield = best_investment['RENT_TO_PRICE_RATIO'] * 12 * 100
                        
                        st.markdown("#### Best Investment Property")
                        st.markdown(f"**{annual_yield:.2f}% yield** - {best_investment['FORMATTED_ADDRESS']}")
                        st.markdown(f"Price: ${best_investment['PRICE']:,.0f} | Est. Rent: ${best_investment['PREDICTED_RENT_PRICE']:,.0f}/mo")
                
                # Show market overview WITHOUT expandable section
                st.markdown("<hr style='margin-top: 30px; margin-bottom: 30px;'>", unsafe_allow_html=True)
                st.subheader("Market Statistics")
                # Display market statistics directly without the expander
                display_property_statistics_main(filtered_data, st.session_state.listing_type)
        else:
            st.info("No properties found matching your criteria. Try adjusting your filters.")
        
        progress_bar.progress(100)
        progress_bar.empty()
    else:
        st.error("No data available. Please check your database connection.")
    
    # Always render database indicator at the very end
    render_db_indicator()

if __name__ == "__main__":
    main()



