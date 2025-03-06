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
MAX_VISIBLE_MARKERS = 1000  # Limit markers for better performance
ENABLE_DATA_SAMPLING = True  # Sample data for faster rendering
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
def load_property_data(table_name, limit=1000):
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
            LIMIT {limit}
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
            LIMIT {limit}
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
            # Create a map centered on this property
            property_map = folium.Map(location=[lat, lon], zoom_start=15, tiles="OpenStreetMap")
            
            # Create a marker for this property
            popup_html = f"""
            <strong>{address}</strong><br>
            ${price:,.0f}<br>
            {beds} bed, {baths} bath, {sqft:,} sq ft
            """
            
            folium.Marker(
                [lat, lon],
                popup=popup_html,
                tooltip=address,
                icon=folium.Icon(color='red', icon='home', prefix='fa')
            ).add_to(property_map)
            
            # Display the map
            folium_static(property_map, width=1200, height=600)
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

def create_property_map(property_data, listing_type="sale", show_zoning=False):
    """Create an interactive map with property markers"""
    if property_data is None or property_data.empty:
        # Return a default Seattle map if no data
        return folium.Map(location=[47.6062, -122.3321], zoom_start=12)
    
    # Calculate the center of the map based on data
    center_lat = property_data['LATITUDE'].mean()
    center_lon = property_data['LONGITUDE'].mean()
    
    # Create a map centered on the properties
    property_map = folium.Map(location=[center_lat, center_lon], zoom_start=12)
    
    # Add a marker cluster for better performance with many markers
    marker_cluster = MarkerCluster().add_to(property_map)
    
    # Zoning layer group (only show if enabled)
    if show_zoning:
        zoning_group = folium.FeatureGroup(name="Zoning Areas", show=True)
        property_map.add_child(zoning_group)
        
        # Add zoning areas
        added_zones = set()  # Track which zones we've already added
        
        for _, property_row in property_data.iterrows():
            zoning_code = property_row.get('ZONING_CODE')
            polygon_geojson = property_row.get('POLYGON_GEOJSON')
            
            # Only add each unique zoning code once
            if zoning_code and polygon_geojson and zoning_code not in added_zones:
                try:
                    # Parse the GeoJSON
                    geojson_data = json.loads(polygon_geojson)
                    
                    # Generate a color based on the zoning code
                    color = generate_color_from_string(zoning_code)
                    
                    # Add the zoning area to the map
                    folium.GeoJson(
                        geojson_data,
                        name=f"Zoning: {zoning_code}",
                        style_function=lambda x, color=color: {
                            'fillColor': color,
                            'color': 'black',
                            'weight': 1,
                            'fillOpacity': 0.4
                        },
                        tooltip=f"Zoning: {zoning_code} - {property_row.get('ZONING_GROUP', 'Unknown')}",
                    ).add_to(zoning_group)
                    
                    added_zones.add(zoning_code)
                except Exception as e:
                    # Silently continue if we can't parse a particular polygon
                    continue
    
    # Add investment quality legend for sale listings
    if listing_type == "sale":
        # Add a custom legend control
        property_map.add_child(Legend())
    
    # Add property markers
    for idx, property_row in property_data.iterrows():
        # Skip properties without coordinates
        if pd.isna(property_row['LATITUDE']) or pd.isna(property_row['LONGITUDE']):
            continue
        
        # Get property information - use safe access with defaults
        bedrooms = int(property_row['BEDROOMS']) if pd.notna(property_row['BEDROOMS']) else 0
        bathrooms = property_row['BATHROOMS'] if pd.notna(property_row['BATHROOMS']) else 0
        sqft = int(property_row['SQUARE_FOOTAGE']) if pd.notna(property_row['SQUARE_FOOTAGE']) else 0
        price = property_row['PRICE'] if pd.notna(property_row['PRICE']) else 0
        address = property_row['FORMATTED_ADDRESS']
        prop_type = property_row.get('PROPERTY_TYPE', 'Unknown')
        year_built = int(property_row.get('YEAR_BUILT', 0)) if pd.notna(property_row.get('YEAR_BUILT')) else 'N/A'
        
        # Determine icon color based on listing type and property data
        icon_color = 'blue'  # Default color
        
        # Simplified color logic for better reliability
        if listing_type == "sale" and 'RENT_TO_PRICE_RATIO' in property_row:
            ratio_value = property_row['RENT_TO_PRICE_RATIO']
            if pd.notna(ratio_value):
                ratio = ratio_value * 12 * 100  # Annual percentage
                
                if ratio > 10:
                    icon_color = 'green'        # Excellent yield (>10%)
                elif ratio > 8:
                    icon_color = 'lightgreen'   # Good yield (8-10%)
                elif ratio > 6:
                    icon_color = 'orange'       # Average yield (6-8%)
                else:
                    icon_color = 'red'          # Below average (<6%)
        
        # Format the price for display
        formatted_price = f"${price/1000:.0f}K" if price < 1000000 else f"${price/1000000:.1f}M"
        
        # Create comprehensive popup with all property details
        popup_html = f"""
        <div style="width: 400px; max-height: 500px; overflow-y: auto; font-family: Arial, sans-serif; padding: 10px;">
            <h3 style="margin-top: 0; color: #2c3e50;">{address}</h3>
            <h2 style="color: #3498db; margin-top: 5px; margin-bottom: 10px;">${price:,.0f}</h2>
            
            <div style="background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin-bottom: 15px;">
                <div style="font-size: 18px; font-weight: bold;">
                    {bedrooms} bed • {bathrooms} bath • {sqft:,} sq ft
                </div>
                <div style="margin-top: 5px;">
                    {prop_type} • Built: {year_built}
                </div>
            </div>
        """
        
        # Add more property details to popup if available
        detail_rows = []
        
        # Add lot size if available
        if 'LOT_SIZE' in property_row and pd.notna(property_row['LOT_SIZE']):
            lot_size = int(property_row['LOT_SIZE'])
            detail_rows.append(f"<tr><td>Lot Size</td><td>{lot_size:,} sq ft</td></tr>")
        
        # Add city and zip if available
        if 'CITY' in property_row and pd.notna(property_row['CITY']):
            city = property_row['CITY']
            if 'ZIP_CODE' in property_row and pd.notna(property_row['ZIP_CODE']):
                city += f", {property_row['ZIP_CODE']}"
            detail_rows.append(f"<tr><td>Location</td><td>{city}</td></tr>")
        
        # Add investment metrics if this is a sale listing
        if listing_type == "sale" and 'PREDICTED_RENT_PRICE' in property_row and pd.notna(property_row['PREDICTED_RENT_PRICE']):
            est_rent = property_row['PREDICTED_RENT_PRICE']
            detail_rows.append(f"<tr><td>Est. Monthly Rent</td><td>${est_rent:,.0f}</td></tr>")
            
            if 'RENT_TO_PRICE_RATIO' in property_row and pd.notna(property_row['RENT_TO_PRICE_RATIO']):
                annual_yield = property_row['RENT_TO_PRICE_RATIO'] * 12 * 100
                yield_color = '#e74c3c'  # Default red
                if annual_yield > 10:
                    yield_color = '#2ecc71'  # Green for excellent
                elif annual_yield > 8:
                    yield_color = '#27ae60'  # Lighter green for good
                elif annual_yield > 6:
                    yield_color = '#f39c12'  # Orange for average
                
                detail_rows.append(f"<tr><td>Annual Yield</td><td><span style='color: {yield_color}; font-weight: bold;'>{annual_yield:.2f}%</span></td></tr>")
                
                # Add estimated mortgage calculation
                mortgage_payment = (price * 0.8 * (0.05/12) * (1 + 0.05/12)**(30*12)) / ((1 + 0.05/12)**(30*12) - 1)
                detail_rows.append(f"<tr><td>Est. Mortgage</td><td>${mortgage_payment:,.0f}/mo</td></tr>")
                
                # Calculate estimated cash flow
                monthly_expenses = price * 0.02 / 12  # Roughly 2% annually for taxes, insurance, maintenance
                cash_flow = est_rent - mortgage_payment - monthly_expenses
                cash_flow_color = '#2ecc71' if cash_flow > 0 else '#e74c3c'
                
                detail_rows.append(f"<tr><td>Est. Cash Flow</td><td><span style='color: {cash_flow_color}; font-weight: bold;'>${cash_flow:,.0f}/mo</span></td></tr>")
        
        # Add price per square foot
        if sqft > 0:
            price_per_sqft = price / sqft
            detail_rows.append(f"<tr><td>Price/SqFt</td><td>${price_per_sqft:.2f}</td></tr>")
        
        # Add details to popup if we have any
        if detail_rows:
            popup_html += """
            <h4 style="margin-top: 15px; margin-bottom: 10px; border-bottom: 1px solid #e0e0e0; padding-bottom: 5px;">
                Property Details
            </h4>
            <table style="width: 100%; border-collapse: collapse;">
            """
            popup_html += "\n".join(detail_rows)
            popup_html += "</table>"
        
        # Add external links
        encoded_address = urllib.parse.quote(address)
        google_url = f"https://www.google.com/search?q={encoded_address}"
        zillow_url = f"https://www.zillow.com/homes/{encoded_address}_rb/"
        maps_url = f"https://www.google.com/maps/search/?api=1&query={encoded_address}"
        
        popup_html += f"""
        <div style="margin-top: 15px; text-align: center;">
            <a href="{google_url}" target="_blank" style="display: inline-block; margin: 0 5px; color: #3498db; text-decoration: none;">
                <strong>🔍 Google</strong>
            </a>
            <a href="{zillow_url}" target="_blank" style="display: inline-block; margin: 0 5px; color: #3498db; text-decoration: none;">
                <strong>🏠 Zillow</strong>
            </a>
            <a href="{maps_url}" target="_blank" style="display: inline-block; margin: 0 5px; color: #3498db; text-decoration: none;">
                <strong>🗺️ Maps</strong>
            </a>
        </div>
        </div>
        """
        
        # Create a simple tooltip
        tooltip = f"{address}: {formatted_price}"
        
        # Create a custom icon with price tag
        try:
            custom_icon = folium.DivIcon(
                html=f'''
                <div>
                    <div style="position: relative;">
                        <i class="fa fa-home" style="font-size: 24px; color: {icon_color};"></i>
                        <div style="position: absolute; top: -10px; left: 20px; 
                             background-color: {icon_color}; color: white; 
                             padding: 2px 4px; font-size: 10px; 
                             border-radius: 3px; white-space: nowrap;">
                            {formatted_price}
                        </div>
                    </div>
                </div>
                ''',
                icon_size=(50, 30),
                icon_anchor=(15, 15),
                class_name="custom-div-icon"
            )
            
            folium.Marker(
                location=[property_row['LATITUDE'], property_row['LONGITUDE']],
                popup=folium.Popup(popup_html, max_width=450),
                tooltip=tooltip,
                icon=custom_icon
            ).add_to(marker_cluster)
        except Exception as e:
            # Fallback to standard icon if DivIcon fails
            folium.Marker(
                location=[property_row['LATITUDE'], property_row['LONGITUDE']],
                popup=folium.Popup(popup_html, max_width=450),
                tooltip=tooltip,
                icon=folium.Icon(icon='home', prefix='fa', color=icon_color)
            ).add_to(marker_cluster)
    
    # Add layer control
    folium.LayerControl().add_to(property_map)
    
    return property_map

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
    
    # Show zoning overlay option - keep in sidebar for consistency
    show_zoning = st.sidebar.checkbox("Enable Zoning Overlays", value=False)
    
    # Progress bar for better UX
    progress_bar = st.progress(0)
    progress_bar.progress(10)
    
    # Load data first
    data = load_property_data(selected_table)
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
            
            # Show zoning option only for sales properties if available
            if show_zoning and st.session_state.listing_type == "sale" and 'ZONING_CODE' in filtered_data.columns:
                show_zoning_toggle = True
                st.caption("Zoning overlays are enabled. They will appear on the map as colored areas.")
            else:
                show_zoning_toggle = False
            
            # Get property map
            property_map = create_property_map(filtered_data, st.session_state.listing_type, show_zoning_toggle)
            
            # Display the map with full width
            folium_static(property_map, width=1350, height=700)
            
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
