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
            FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name} r
            JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_PROPERTY p 
                ON r.PROPERTY_SK = p.PROPERTY_SK
            LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_ZONING z
                ON p.ZONING_ID = z.ZONING_ID
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
            FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name} r
            JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_PROPERTY p 
                ON r.PROPERTY_SK = p.PROPERTY_SK
            LEFT JOIN DATAEXPERT_STUDENT.JMUSNI07.DIM_ZONING z
                ON p.ZONING_ID = z.ZONING_ID
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
                          'LATITUDE', 'LONGITUDE']
            
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
    """Display detailed information about a selected property with Google search functionality"""
    if property_data is None:
        st.warning("No property data available")
        return
        
    # Extract key information
    prop_type = property_data.get('PROPERTY_TYPE', 'Property')
    address = property_data.get('FORMATTED_ADDRESS', 'Address not available')
    price = property_data.get('PRICE')
    price_display = f"${price:,.0f}" if price and not pd.isna(price) else "Price not available"
    
    # Display property header with type and address
    st.subheader(f"{prop_type} at {address}")
    st.markdown(f"### {price_display}")
    
    # Create columns for property details
    col1, col2 = st.columns(2)
    
    # First column with basic property information
    with col1:
        # Display property specs
        specs = [
            ("Bedrooms", property_data.get('BEDROOMS')),
            ("Bathrooms", property_data.get('BATHROOMS')),
            ("Square Footage", property_data.get('SQUARE_FOOTAGE')),
            ("Year Built", property_data.get('YEAR_BUILT')),
            ("Days on Market", property_data.get('DAYS_ON_MARKET'))
        ]
        
        for label, value in specs:
            if value is not None and not pd.isna(value):
                formatted_value = f"{value:,}" if isinstance(value, (int, float)) else value
                st.markdown(f"**{label}:** {formatted_value}")
        
        # Add price per square foot if both price and square footage are available
        if (price is not None and not pd.isna(price) and 
            property_data.get('SQUARE_FOOTAGE') is not None and 
            not pd.isna(property_data.get('SQUARE_FOOTAGE')) and
            property_data.get('SQUARE_FOOTAGE') > 0):
            price_per_sqft = price / property_data.get('SQUARE_FOOTAGE')
            st.markdown(f"**Price per Sq Ft:** ${price_per_sqft:.2f}")
    
    # Second column with zoning and location information
    with col2:
        # Zoning information
        zoning_code = property_data.get('ZONING_CODE')
        zoning_group = property_data.get('ZONING_GROUP')
        
        if zoning_code is not None and not pd.isna(zoning_code):
            st.markdown(f"**Zoning Code:** {zoning_code}")
        if zoning_group is not None and not pd.isna(zoning_group):
            st.markdown(f"**Zoning Type:** {zoning_group}")
            
        # Additional property details
        if 'LOT_SIZE' in property_data and not pd.isna(property_data['LOT_SIZE']):
            st.markdown(f"**Lot Size:** {property_data['LOT_SIZE']:,} sq ft")
            
        # Last sale information for sale properties
        if 'LAST_SALE_PRICE' in property_data and not pd.isna(property_data['LAST_SALE_PRICE']):
            st.markdown(f"**Last Sale Price:** ${property_data['LAST_SALE_PRICE']:,.0f}")
        if 'LAST_SALE_DATE' in property_data and not pd.isna(property_data['LAST_SALE_DATE']):
            st.markdown(f"**Last Sale Date:** {property_data['LAST_SALE_DATE']}")
    
    # Add a horizontal rule to separate sections
    st.markdown("---")
    
    # Restore Google search functionality
    st.subheader("Research This Property")
    
    # Create search buttons for different platforms
    search_col1, search_col2, search_col3 = st.columns(3)
    
    # Encode the address for URL
    encoded_address = urllib.parse.quote(address)
    
    # Google Maps search
    with search_col1:
        maps_url = f"https://www.google.com/maps/search/?api=1&query={encoded_address}"
        st.markdown(f"[![Google Maps](https://img.shields.io/badge/Google_Maps-4285F4?style=for-the-badge&logo=google-maps&logoColor=white)]({maps_url})")
    
    # Google search
    with search_col2:
        google_url = f"https://www.google.com/search?q={encoded_address}"
        st.markdown(f"[![Google Search](https://img.shields.io/badge/Google_Search-4285F4?style=for-the-badge&logo=google&logoColor=white)]({google_url})")
    
    # Zillow search (if it's a residential property)
    with search_col3:
        zillow_url = f"https://www.zillow.com/homes/{encoded_address}_rb/"
        st.markdown(f"[![Zillow](https://img.shields.io/badge/Zillow-006AFF?style=for-the-badge&logo=zillow&logoColor=white)]({zillow_url})")
    
    # Add a section for nearby amenities search
    st.markdown("### Nearby Amenities")
    amenity_col1, amenity_col2, amenity_col3 = st.columns(3)
    
    # Restaurants nearby
    with amenity_col1:
        restaurants_url = f"https://www.google.com/maps/search/restaurants+near+{encoded_address}"
        st.markdown(f"[🍽️ Restaurants]({restaurants_url})")
    
    # Schools nearby
    with amenity_col2:
        schools_url = f"https://www.google.com/maps/search/schools+near+{encoded_address}"
        st.markdown(f"[🏫 Schools]({schools_url})")
    
    # Parks nearby
    with amenity_col3:
        parks_url = f"https://www.google.com/maps/search/parks+near+{encoded_address}"
        st.markdown(f"[🌳 Parks]({parks_url})")
    
    return

# ======= CREATE PROPERTY MAP =======
def create_property_map(property_data, listing_type, show_zoning=False):
    """Create a Folium map with price markers and working zoning overlays"""
    import folium
    from folium.plugins import MarkerCluster
    import json
    import urllib.parse
    import colorsys
    
    # Check if data is available
    if property_data is None or property_data.empty:
        # Create a default map centered on San Francisco
        m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)
        folium.Marker(
            [37.7749, -122.4194],
            popup="No property data available",
            icon=folium.Icon(color="red", icon="info-sign"),
        ).add_to(m)
        return m
    
    # Find center of the map based on properties
    center_lat = property_data['LATITUDE'].mean()
    center_lon = property_data['LONGITUDE'].mean()
    
    # Create base map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
    
    # Add marker cluster
    marker_cluster = MarkerCluster().add_to(m)
    
    # Process zoning data first if requested
    if show_zoning:
        try:
            # Create a zoning layer that will be added to the map
            zoning_layer = folium.FeatureGroup(name="Zoning Areas", show=True)
            
            # Get unique zoning codes
            if 'ZONING_CODE' in property_data.columns:
                unique_zones = property_data['ZONING_CODE'].dropna().unique()
                
                # Generate color for each zone
                zone_colors = {}
                for i, zone in enumerate(unique_zones):
                    # Generate a color based on the zoning code hash
                    hue = (hash(str(zone)) % 100) / 100.0
                    # Create a lighter, pastel-like color
                    r, g, b = colorsys.hls_to_rgb(hue, 0.7, 0.8)
                    # Convert to hex color
                    zone_colors[zone] = f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'
                
                # Process each property that has polygon data
                zoning_polygons_added = False
                for _, prop in property_data.iterrows():
                    if 'POLYGON_GEOJSON' in prop and not pd.isna(prop['POLYGON_GEOJSON']) and prop['POLYGON_GEOJSON']:
                        try:
                            # Parse the GeoJSON string
                            if isinstance(prop['POLYGON_GEOJSON'], str):
                                # Print for debugging
                                if prop['POLYGON_GEOJSON'].strip():  # Only try to parse if not empty
                                    # Clean the GeoJSON string if necessary
                                    geojson_str = prop['POLYGON_GEOJSON'].strip()
                                    geojson_data = json.loads(geojson_str)
                                    
                                    zone = str(prop.get('ZONING_CODE', 'Unknown'))
                                    color = zone_colors.get(zone, '#CCCCCC')
                                    
                                    # Add the polygon
                                    folium.GeoJson(
                                        geojson_data,
                                        style_function=lambda x, color=color: {
                                            'fillColor': color,
                                            'color': 'black',
                                            'weight': 1,
                                            'fillOpacity': 0.4
                                        },
                                        tooltip=f"Zoning: {zone}"
                                    ).add_to(zoning_layer)
                                    zoning_polygons_added = True
                        except Exception as e:
                            # Skip this polygon on error but log it
                            print(f"Error adding zoning polygon: {str(e)}")
                            continue
                
                # Only add the layer if we successfully added polygons
                if zoning_polygons_added:
                    zoning_layer.add_to(m)
                    print("Successfully added zoning layer to map")
                else:
                    print("No valid zoning polygons found to add to map")
            else:
                print("No ZONING_CODE column found in data")
        except Exception as e:
            print(f"Error setting up zoning layer: {str(e)}")
    
    # Add properties to the map
    for _, prop in property_data.iterrows():
        try:
            # Skip if coordinates are missing
            if pd.isna(prop['LATITUDE']) or pd.isna(prop['LONGITUDE']):
                continue
            
            # Format property information
            prop_type = prop.get('PROPERTY_TYPE', 'Property')
            address = prop.get('FORMATTED_ADDRESS', 'Address not available')
            price = prop.get('PRICE', 0)
            
            # Format price nicely
            if pd.isna(price):
                price_formatted = "N/A"
                price_short = "N/A"
            else:
                price_formatted = f"${price:,.0f}"
                # Create a short version for the marker (K for thousands, M for millions)
                if price >= 1000000:
                    price_short = f"${price/1000000:.1f}M"
                else:
                    price_short = f"${price/1000:.0f}K"
            
            # Create a popup with property details
            popup_html = f"""
            <div style="min-width: 200px; max-width: 300px;">
                <h4>{prop_type}</h4>
                <h3>{price_formatted}</h3>
                <p><b>Address:</b> {address}</p>
            """
            
            # Add additional property details if available
            if 'BEDROOMS' in prop and not pd.isna(prop['BEDROOMS']):
                popup_html += f"<p><b>Beds:</b> {prop['BEDROOMS']}</p>"
            if 'BATHROOMS' in prop and not pd.isna(prop['BATHROOMS']):
                popup_html += f"<p><b>Baths:</b> {prop['BATHROOMS']}</p>"
            if 'SQUARE_FOOTAGE' in prop and not pd.isna(prop['SQUARE_FOOTAGE']):
                popup_html += f"<p><b>Sq Ft:</b> {prop['SQUARE_FOOTAGE']:,.0f}</p>"
            if 'DAYS_ON_MARKET' in prop and not pd.isna(prop['DAYS_ON_MARKET']):
                popup_html += f"<p><b>Days on Market:</b> {prop['DAYS_ON_MARKET']}</p>"
            
            # Add zoning info to popup if available
            if 'ZONING_CODE' in prop and not pd.isna(prop['ZONING_CODE']):
                popup_html += f"<p><b>Zoning:</b> {prop['ZONING_CODE']}</p>"
            
            # Add Google search links
            encoded_address = urllib.parse.quote(address)
            popup_html += f"""
            <div style="margin-top: 10px;">
                <b>Search:</b><br>
                <a href="https://www.google.com/maps/search/?api=1&query={encoded_address}" target="_blank">Google Maps</a> | 
                <a href="https://www.google.com/search?q={encoded_address}" target="_blank">Google</a> | 
                <a href="https://www.zillow.com/homes/{encoded_address}_rb/" target="_blank">Zillow</a>
            </div>
            """
            
            # Add View Details button
            if 'LISTING_ID' in prop:
                listing_id = prop['LISTING_ID']
                popup_html += f"""
                <div style="margin-top: 10px; text-align: center;">
                    <a href="#" onclick="parent.postMessage({{action: 'selectProperty', listingId: '{listing_id}'}}, '*'); return false;" 
                    style="display: inline-block; padding: 5px 15px; background-color: #4285F4; color: white; text-decoration: none; border-radius: 3px;">
                    View Full Details</a>
                </div>
                """
            
            popup_html += "</div>"
            
            # Determine marker color based on listing type
            bg_color = "#4CAF50" if listing_type == "sale" else "#2196F3"  # Green for sales, Blue for rentals
            
            # Create custom HTML for the price marker
            price_marker_html = f"""
                <div style="
                    background-color: {bg_color}; 
                    color: white; 
                    font-weight: bold; 
                    padding: 3px 8px; 
                    border-radius: 10px; 
                    font-size: 12px; 
                    box-shadow: 0 1px 3px rgba(0,0,0,0.3);
                    white-space: nowrap;
                    text-align: center;
                ">
                    {price_short}
                </div>
            """
            
            # Add marker to cluster with custom price icon
            folium.Marker(
                [prop['LATITUDE'], prop['LONGITUDE']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{prop_type}: {price_formatted}",
                icon=folium.DivIcon(
                    html=price_marker_html,
                    icon_size=(100, 20),  # Adjust size as needed
                    icon_anchor=(50, 10)  # Center of the icon
                )
            ).add_to(marker_cluster)
            
        except Exception as e:
            # Skip this property on error
            continue
    
    # Add layer control to toggle layers
    folium.LayerControl().add_to(m)
    
    # Add a diagnostic message about zoning on the map
    if show_zoning:
        folium.Marker(
            [center_lat, center_lon],
            popup="Zoning data should be visible. Toggle layers using the control in the top-right corner.",
            icon=folium.Icon(color="purple", icon="info-sign")
        ).add_to(m)
    
    return m

# ======= PROPERTY ANALYTICS FUNCTIONS =======
def calculate_market_stats(property_data):
    """Calculate key market statistics from property data"""
    if property_data.empty:
        return None
    
    stats = {}
    
    # Price statistics
    if 'PRICE' in property_data.columns:
        price_data = property_data['PRICE'].dropna()
        if not price_data.empty:
            stats['median_price'] = price_data.median()
            stats['avg_price'] = price_data.mean()
            stats['min_price'] = price_data.min()
            stats['max_price'] = price_data.max()
            stats['price_std'] = price_data.std()
    
    # Price per square foot
    if 'PRICE' in property_data.columns and 'SQUARE_FOOTAGE' in property_data.columns:
        # Calculate price per square foot for each property
        property_data['PRICE_PER_SQFT'] = property_data.apply(
            lambda row: row['PRICE'] / row['SQUARE_FOOTAGE'] if pd.notna(row['PRICE']) and pd.notna(row['SQUARE_FOOTAGE']) and row['SQUARE_FOOTAGE'] > 0 else None, 
            axis=1
        )
        ppsf_data = property_data['PRICE_PER_SQFT'].dropna()
        if not ppsf_data.empty:
            stats['median_price_per_sqft'] = ppsf_data.median()
            stats['avg_price_per_sqft'] = ppsf_data.mean()
    
    # Bedrooms breakdown
    if 'BEDROOMS' in property_data.columns:
        beds_data = property_data['BEDROOMS'].dropna()
        if not beds_data.empty:
            stats['avg_beds'] = beds_data.mean()
            stats['beds_counts'] = beds_data.value_counts().sort_index().to_dict()
    
    # Days on market
    if 'DAYS_ON_MARKET' in property_data.columns:
        dom_data = property_data['DAYS_ON_MARKET'].dropna()
        if not dom_data.empty:
            stats['avg_days_on_market'] = dom_data.mean()
            stats['median_days_on_market'] = dom_data.median()
    
    # Zoning breakdown
    if 'ZONING_CODE' in property_data.columns:
        zoning_data = property_data['ZONING_CODE'].dropna()
        if not zoning_data.empty:
            stats['zoning_counts'] = zoning_data.value_counts().to_dict()
    
    # Property type breakdown
    if 'PROPERTY_TYPE' in property_data.columns:
        type_data = property_data['PROPERTY_TYPE'].dropna()
        if not type_data.empty:
            stats['property_type_counts'] = type_data.value_counts().to_dict()
    
    return stats

def display_property_analytics(property_data, listing_type="sale"):
    """Display property analytics in the right sidebar"""
    if property_data.empty:
        st.sidebar.warning("No data available for analytics")
        return
    
    # Calculate market statistics
    stats = calculate_market_stats(property_data)
    if not stats:
        st.sidebar.warning("Insufficient data for analytics")
        return
    
    # Set title based on listing type
    title = "Market Analytics: For Sale" if listing_type == "sale" else "Market Analytics: For Rent"
    st.sidebar.title(title)
    
    # Show total properties count
    st.sidebar.metric("Total Listings", len(property_data))
    
    # Price summary section
    st.sidebar.subheader("Price Summary")
    
    # Format price labels based on listing type
    price_label = "Price" if listing_type == "sale" else "Monthly Rent"
    
    # Create 3-column layout for price metrics
    col1, col2, col3 = st.sidebar.columns(3)
    
    if 'median_price' in stats:
        col1.metric(f"Median {price_label}", f"${stats['median_price']:,.0f}")
    if 'min_price' in stats:
        col2.metric(f"Min {price_label}", f"${stats['min_price']:,.0f}")
    if 'max_price' in stats:
        col3.metric(f"Max {price_label}", f"${stats['max_price']:,.0f}")
    
    # Price per square foot
    if 'median_price_per_sqft' in stats:
        st.sidebar.metric("Median Price/SqFt", f"${stats['median_price_per_sqft']:.2f}")
    
    # Days on market
    if 'median_days_on_market' in stats:
        st.sidebar.metric("Median Days on Market", f"{stats['median_days_on_market']:.0f} days")
    
    # Create a price distribution chart
    if 'PRICE' in property_data.columns and not property_data['PRICE'].dropna().empty:
        st.sidebar.subheader("Price Distribution")
        
        # Create bins for price histogram
        price_data = property_data['PRICE'].dropna()
        fig = px.histogram(
            price_data, 
            nbins=10,
            labels={'value': price_label, 'count': 'Number of Listings'},
            title=f"{price_label} Distribution"
        )
        fig.update_layout(
            showlegend=False,
            margin=dict(l=10, r=10, t=30, b=10),
            height=200
        )
        st.sidebar.plotly_chart(fig, use_container_width=True)
    
    # Property characteristics breakdown
    st.sidebar.subheader("Property Breakdown")
    
    # Show property types in a horizontal bar chart
    if 'property_type_counts' in stats:
        # Convert to DataFrame for Plotly
        type_df = pd.DataFrame({
            'Type': list(stats['property_type_counts'].keys()),
            'Count': list(stats['property_type_counts'].values())
        }).sort_values('Count', ascending=False)
        
        if len(type_df) > 0:
            fig = px.bar(
                type_df, 
                x='Count', 
                y='Type',
                orientation='h',
                title='Property Types'
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=10, r=10, t=30, b=10),
                height=200
            )
            st.sidebar.plotly_chart(fig, use_container_width=True)
    
    # Bedroom distribution in a pie chart
    if 'beds_counts' in stats:
        # Convert to DataFrame for Plotly
        beds_df = pd.DataFrame({
            'Bedrooms': list(stats['beds_counts'].keys()),
            'Count': list(stats['beds_counts'].values())
        })
        
        if len(beds_df) > 0:
            fig = px.pie(
                beds_df, 
                values='Count', 
                names='Bedrooms',
                title='Bedroom Distribution'
            )
            fig.update_layout(
                margin=dict(l=10, r=10, t=30, b=10),
                height=200
            )
            st.sidebar.plotly_chart(fig, use_container_width=True)
    
    # Zoning breakdown
    if 'zoning_counts' in stats and len(stats['zoning_counts']) > 0:
        st.sidebar.subheader("Zoning Analysis")
        
        # Convert to DataFrame for Plotly
        zoning_df = pd.DataFrame({
            'Zoning': list(stats['zoning_counts'].keys()),
            'Count': list(stats['zoning_counts'].values())
        }).sort_values('Count', ascending=False)
        
        # Limit to top 5 for clarity
        if len(zoning_df) > 5:
            zoning_df = zoning_df.head(5)
        
        fig = px.bar(
            zoning_df, 
            x='Zoning', 
            y='Count',
            title='Top Zoning Types'
        )
        fig.update_layout(
            xaxis_title='',
            showlegend=False,
            margin=dict(l=10, r=10, t=30, b=10),
            height=200
        )
        st.sidebar.plotly_chart(fig, use_container_width=True)
    
    # Show selected property details when a property is clicked
    st.sidebar.subheader("Selected Property")
    if 'selected_property' in st.session_state and st.session_state.selected_property is not None:
        prop = st.session_state.selected_property
        
        # Create an expander for the property details
        with st.sidebar.expander("Property Details", expanded=True):
            # Price
            if 'PRICE' in prop and pd.notna(prop['PRICE']):
                st.markdown(f"### ${prop['PRICE']:,.0f}")
            
            # Address
            address_parts = []
            for field in ['ADDRESS_LINE_1', 'CITY', 'STATE', 'ZIP_CODE']:
                if field in prop and pd.notna(prop[field]):
                    address_parts.append(str(prop[field]))
            
            if address_parts:
                st.markdown("#### " + ", ".join(address_parts))
            
            # Property characteristics
            details = []
            if 'BEDROOMS' in prop and pd.notna(prop['BEDROOMS']):
                details = safe_append(details, f"{int(prop['BEDROOMS'])} beds")
            if 'BATHROOMS' in prop and pd.notna(prop['BATHROOMS']):
                bath_val = prop['BATHROOMS']
                if bath_val == int(bath_val):
                    details = safe_append(details, f"{int(bath_val)} baths")
                else:
                    details = safe_append(details, f"{bath_val} baths")
            if 'SQUARE_FOOTAGE' in prop and pd.notna(prop['SQUARE_FOOTAGE']):
                details = safe_append(details, f"{int(prop['SQUARE_FOOTAGE']):,} sq ft")
            
            if details:
                st.markdown("**" + " | ".join(details) + "**")
            
            # Property type
            if 'PROPERTY_TYPE' in prop and pd.notna(prop['PROPERTY_TYPE']):
                st.markdown(f"**Type:** {prop['PROPERTY_TYPE']}")
            
            # Year built
            if 'YEAR_BUILT' in prop and pd.notna(prop['YEAR_BUILT']):
                st.markdown(f"**Year Built:** {int(prop['YEAR_BUILT'])}")
            
            # Days on market
            if 'DAYS_ON_MARKET' in prop and pd.notna(prop['DAYS_ON_MARKET']):
                st.markdown(f"**Days on Market:** {int(prop['DAYS_ON_MARKET'])}")
            
            # Zoning
            if 'ZONING_CODE' in prop and pd.notna(prop['ZONING_CODE']):
                zoning_group = prop['ZONING_GROUP'] if 'ZONING_GROUP' in prop and pd.notna(prop['ZONING_GROUP']) else "Unknown"
                st.markdown(f"**Zoning:** {prop['ZONING_CODE']} ({zoning_group})")
            
            # Last sale info
            if 'LAST_SALE_PRICE' in prop and pd.notna(prop['LAST_SALE_PRICE']) and 'LAST_SALE_DATE' in prop and pd.notna(prop['LAST_SALE_DATE']):
                st.markdown(f"**Last Sale:** ${int(prop['LAST_SALE_PRICE']):,} on {prop['LAST_SALE_DATE']}")
            
            # Calculate relative metrics (comparison to average)
            if 'PRICE' in prop and pd.notna(prop['PRICE']) and 'avg_price' in stats:
                price_diff_pct = (prop['PRICE'] - stats['avg_price']) / stats['avg_price'] * 100
                st.markdown(f"**Price vs. Average:** {price_diff_pct:.1f}%")
            
            if 'SQUARE_FOOTAGE' in prop and pd.notna(prop['SQUARE_FOOTAGE']) and prop['SQUARE_FOOTAGE'] > 0 and 'PRICE' in prop and pd.notna(prop['PRICE']):
                price_per_sqft = prop['PRICE'] / prop['SQUARE_FOOTAGE']
                if 'avg_price_per_sqft' in stats:
                    ppsf_diff_pct = (price_per_sqft - stats['avg_price_per_sqft']) / stats['avg_price_per_sqft'] * 100
                    st.markdown(f"**${price_per_sqft:.2f}/sq ft** ({ppsf_diff_pct:.1f}% vs avg)")
    else:
        st.sidebar.info("Click on a property marker to see details")

def display_property_statistics_main(property_data, listing_type="sale", container=None):
    """Display property statistics with a completely flat layout to avoid nested columns"""
    # Use the provided container or st directly
    display = container if container else st
    
    if property_data is None or property_data.empty:
        display.warning("No data available for statistics")
        return
    
    # Custom CSS for basic styling only
    display.markdown("""
    <style>
    .small-font {
        font-size: 0.9rem !important;
        margin-bottom: 0.2rem !important;
    }
    .section-header {
        font-size: 1.1rem !important;
        font-weight: bold !important;
        margin-bottom: 0.3rem !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Set title based on listing type with more compact styling
    title_text = "Sales Market Overview" if listing_type == "sale" else "Rental Market Overview"
    display.markdown(f"## {title_text}")
    
    # Show total properties count
    display.markdown(f"**Showing {len(property_data)} properties**")
    
    # --- MAIN STATISTICS ---
    # Create a 6-column grid for all metrics at once (no nesting)
    all_cols = display.columns(6)
    
    # --- PRICE STATISTICS ---
    all_cols[0].markdown("<p class='section-header'>Price Stats</p>", unsafe_allow_html=True)
    
    if 'PRICE' in property_data.columns:
        price_data = property_data['PRICE'].dropna()
        if not price_data.empty:
            median_price = price_data.median()
            mean_price = price_data.mean()
            min_price = price_data.min()
            max_price = price_data.max()
            
            # Format price values
            median_price_fmt = f"${median_price:,.0f}"
            mean_price_fmt = f"${mean_price:,.0f}"
            price_range_fmt = f"${min_price:,.0f} - ${max_price:,.0f}"
            
            # Use separate columns from our 6-column grid
            all_cols[0].metric("Median", median_price_fmt)
            all_cols[1].metric("Average", mean_price_fmt)
            all_cols[0].caption(f"Range: {price_range_fmt}")
    
    # --- PROPERTY CHARACTERISTICS ---
    all_cols[2].markdown("<p class='section-header'>Property</p>", unsafe_allow_html=True)
    
    # Bedrooms distribution
    if 'BEDROOMS' in property_data.columns:
        avg_beds = property_data['BEDROOMS'].mean()
        all_cols[2].metric("Avg. Beds", f"{avg_beds:.1f}")
        
        bed_counts = property_data['BEDROOMS'].value_counts().sort_index()
        if not bed_counts.empty:
            bed_summary = " | ".join([f"{int(beds)}br: {count}" for beds, count in bed_counts.items()])
            all_cols[2].caption(bed_summary)
    
    # Property types
    if 'PROPERTY_TYPE' in property_data.columns:
        type_counts = property_data['PROPERTY_TYPE'].value_counts()
        if len(type_counts) > 0:
            top_type = type_counts.index[0]
            top_type_pct = 100 * type_counts.iloc[0] / len(property_data)
            all_cols[3].metric("Top Type", f"{top_type}")
            all_cols[3].caption(f"{top_type_pct:.0f}%")
    
    # --- MARKET METRICS ---
    all_cols[4].markdown("<p class='section-header'>Market</p>", unsafe_allow_html=True)
    
    # Price per square foot
    if 'PRICE' in property_data.columns and 'SQUARE_FOOTAGE' in property_data.columns:
        # Calculate price per square foot
        property_data['PRICE_PER_SQFT'] = property_data.apply(
            lambda row: row['PRICE'] / row['SQUARE_FOOTAGE'] 
            if pd.notna(row['PRICE']) and pd.notna(row['SQUARE_FOOTAGE']) and row['SQUARE_FOOTAGE'] > 0 
            else None,
            axis=1
        )
        
        price_sqft = property_data['PRICE_PER_SQFT'].dropna()
        if not price_sqft.empty:
            median_ppsf = price_sqft.median()
            # Use a separate column
            all_cols[4].metric("Price/SqFt", f"${median_ppsf:.2f}")
    
    # Days on market
    if 'DAYS_ON_MARKET' in property_data.columns:
        dom = property_data['DAYS_ON_MARKET'].dropna()
        if not dom.empty:
            median_dom = dom.median()
            # Use a separate column
            all_cols[5].metric("Days on Mkt", f"{median_dom:.0f}")
    
    # --- SECOND ROW ---
    # Create another row of columns for additional stats
    display.markdown("---") # Add a separator
    row2_cols = display.columns(4)
    
    # Year built
    if 'YEAR_BUILT' in property_data.columns:
        year_built = property_data['YEAR_BUILT'].dropna()
        if not year_built.empty:
            median_year = year_built.median()
            oldest = year_built.min()
            newest = year_built.max()
            row2_cols[0].metric("Median Year", f"{median_year:.0f}")
            row2_cols[0].caption(f"Range: {oldest:.0f} - {newest:.0f}")
    
    # ZIP code distribution
    if 'ZIP_CODE' in property_data.columns:
        zip_counts = property_data['ZIP_CODE'].value_counts().head(3)
        if not zip_counts.empty:
            row2_cols[1].markdown("**Top ZIP Codes:**")
            zip_text = ""
            for zip_code, count in zip_counts.items():
                percentage = 100 * count / len(property_data)
                zip_text += f"- {zip_code}: {count} ({percentage:.1f}%)\n"
            row2_cols[1].markdown(zip_text)
    
    # Zoning information
    if 'ZONING_GROUP' in property_data.columns:
        zoning_counts = property_data['ZONING_GROUP'].value_counts().head(3)
        if not zoning_counts.empty:
            row2_cols[2].markdown("**Zoning Groups:**")
            zoning_text = ""
            for zone, count in zoning_counts.items():
                percentage = 100 * count / len(property_data)
                zoning_text += f"- {zone}: {count} ({percentage:.1f}%)\n"
            row2_cols[2].markdown(zoning_text)
            
    # Square footage
    if 'SQUARE_FOOTAGE' in property_data.columns:
        sqft_data = property_data['SQUARE_FOOTAGE'].dropna()
        if not sqft_data.empty:
            median_sqft = sqft_data.median()
            avg_sqft = sqft_data.mean()
            row2_cols[3].metric("Median SqFt", f"{median_sqft:,.0f}")
            row2_cols[3].caption(f"Avg: {avg_sqft:,.0f}")

# Now update where the property details placeholder was shown
def show_property_details_or_stats():
    """Show selected property details or overall statistics if no property selected"""
    if 'selected_property' in st.session_state and st.session_state.selected_property is not None:
        # Show details for the selected property
        prop = st.session_state.selected_property
        st.title(f"Property Details: {prop.get('ADDRESS_LINE_1', 'N/A')}")
        
        # Display property information in columns
        col1, col2 = st.columns(2)
        
        # Left column: Basic details
        with col1:
            if 'PRICE' in prop:
                st.metric("Price", f"${prop['PRICE']:,.0f}")
            
            details = []
            if 'BEDROOMS' in prop and pd.notna(prop['BEDROOMS']):
                details.append(f"{int(prop['BEDROOMS'])} beds")
            if 'BATHROOMS' in prop and pd.notna(prop['BATHROOMS']):
                details.append(f"{prop['BATHROOMS']} baths")
            if 'SQUARE_FOOTAGE' in prop and pd.notna(prop['SQUARE_FOOTAGE']):
                details.append(f"{int(prop['SQUARE_FOOTAGE']):,} sq ft")
            
            if details:
                st.write(" • ".join(details))
            
            if 'FORMATTED_ADDRESS' in prop:
                st.write(f"📍 {prop['FORMATTED_ADDRESS']}")
            
            if 'YEAR_BUILT' in prop and pd.notna(prop['YEAR_BUILT']):
                st.write(f"🏗️ Built in {int(prop['YEAR_BUILT'])}")
        
        # Right column: Additional info
        with col2:
            if 'DAYS_ON_MARKET' in prop and pd.notna(prop['DAYS_ON_MARKET']):
                st.metric("Days on Market", f"{int(prop['DAYS_ON_MARKET'])}")
            
            if 'PROPERTY_TYPE' in prop:
                st.write(f"🏠 {prop['PROPERTY_TYPE']}")
            
            if 'ZONING_CODE' in prop:
                st.write(f"🏢 Zoning: {prop['ZONING_CODE']}")
            
            # Calculate and show price per square foot
            if 'PRICE' in prop and 'SQUARE_FOOTAGE' in prop and pd.notna(prop['SQUARE_FOOTAGE']) and float(prop['SQUARE_FOOTAGE']) > 0:
                price_per_sqft = prop['PRICE'] / prop['SQUARE_FOOTAGE']
                st.metric("Price per Sq Ft", f"${price_per_sqft:.2f}")
    
    else:
        # No property selected, show overall statistics instead
        if 'property_data' in st.session_state and not st.session_state.property_data.empty:
            listing_type = st.session_state.listing_type if 'listing_type' in st.session_state else "sale"
            display_property_statistics_main(st.session_state.property_data, listing_type)
        else:
            st.info("Apply filters to see property statistics")

# Make sure to call this function where you previously showed the placeholder
# Replace code like this:
#   st.subheader("Property Details")
#   st.info("👈 Click on a property marker to view details")
# With:
show_property_details_or_stats()

# ======= MAIN APP =======
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
    
    # Show zoning overlay option
    show_zoning = st.sidebar.checkbox("Show Zoning Overlay", value=False)
    
    # Progress bar for better UX
    progress_bar = st.progress(0)
    progress_bar.progress(10)
    
    # Load data
    data = load_property_data(selected_table)
    progress_bar.progress(30)
    
    # Check if data is available
    if len(data) > 0:
        # Handle the property filters in the sidebar
        def apply_filters(data):
            """Apply filters to the property data"""
            filtered_data = data.copy()
            
            # Skip filtering if data is empty
            if filtered_data.empty:
                return filtered_data
            
            # Price filter
            if 'PRICE' in filtered_data.columns:
                min_price = float(filtered_data['PRICE'].min()) if not filtered_data['PRICE'].isna().all() else 0
                max_price = float(filtered_data['PRICE'].max()) if not filtered_data['PRICE'].isna().all() else 1000000
                
                # Round to nearest 10k for better UX
                min_price = math.floor(min_price / 10000) * 10000
                max_price = math.ceil(max_price / 10000) * 10000
                
                price_range = st.sidebar.slider(
                    "Price Range ($)",
                    min_value=int(min_price),
                    max_value=int(max_price),
                    value=[int(min_price), int(max_price)],
                    step=10000
                )
                
                filtered_data = filtered_data[
                    (filtered_data['PRICE'] >= price_range[0]) & 
                    (filtered_data['PRICE'] <= price_range[1])
                ]
            
            # Bedrooms filter
            if 'BEDROOMS' in filtered_data.columns:
                bed_options = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                min_beds = st.sidebar.selectbox("Minimum Bedrooms", bed_options, index=0)
                filtered_data = filtered_data[filtered_data['BEDROOMS'] >= min_beds]
            
            # Bathrooms filter
            if 'BATHROOMS' in filtered_data.columns:
                bath_options = [0, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6]
                min_baths = st.sidebar.selectbox("Minimum Bathrooms", bath_options, index=0)
                filtered_data = filtered_data[filtered_data['BATHROOMS'] >= min_baths]
            
            # Square footage filter
            if 'SQUARE_FOOTAGE' in filtered_data.columns:
                min_sqft = float(filtered_data['SQUARE_FOOTAGE'].min()) if not filtered_data['SQUARE_FOOTAGE'].isna().all() else 0
                max_sqft = float(filtered_data['SQUARE_FOOTAGE'].max()) if not filtered_data['SQUARE_FOOTAGE'].isna().all() else 10000
                
                # Round to nearest 100 for better UX
                min_sqft = math.floor(min_sqft / 100) * 100
                max_sqft = math.ceil(max_sqft / 100) * 100
                
                sqft_range = st.sidebar.slider(
                    "Square Footage",
                    min_value=int(min_sqft),
                    max_value=int(max_sqft),
                    value=[int(min_sqft), int(max_sqft)],
                    step=100
                )
                
                filtered_data = filtered_data[
                    (filtered_data['SQUARE_FOOTAGE'] >= sqft_range[0]) & 
                    (filtered_data['SQUARE_FOOTAGE'] <= sqft_range[1])
                ]
            
            # Property type filter
            if 'PROPERTY_TYPE' in filtered_data.columns:
                property_types = ['All Types'] + sorted(filtered_data['PROPERTY_TYPE'].dropna().unique().tolist())
                selected_type = st.sidebar.selectbox("Property Type", property_types)
                
                if selected_type != 'All Types':
                    filtered_data = filtered_data[filtered_data['PROPERTY_TYPE'] == selected_type]
            
            # Days on market filter (if available)
            if 'DAYS_ON_MARKET' in filtered_data.columns:
                max_dom = st.sidebar.slider(
                    "Maximum Days on Market", 
                    min_value=0, 
                    max_value=365, 
                    value=365
                )
                filtered_data = filtered_data[filtered_data['DAYS_ON_MARKET'] <= max_dom]
            
            # ZIP code filter
            if 'ZIP_CODE' in filtered_data.columns:
                zip_codes = ['All ZIP Codes'] + sorted(filtered_data['ZIP_CODE'].dropna().unique().tolist())
                selected_zip = st.sidebar.selectbox("ZIP Code", zip_codes)
                
                if selected_zip != 'All ZIP Codes':
                    filtered_data = filtered_data[filtered_data['ZIP_CODE'] == selected_zip]
            
            return filtered_data
        
        # Filter the data
        filtered_data = apply_filters(data)
        
        # Sample data for better performance if needed
        display_data = filtered_data
        if len(filtered_data) > MAX_VISIBLE_MARKERS and ENABLE_DATA_SAMPLING:
            display_data = filtered_data.sample(MAX_VISIBLE_MARKERS)
            st.info(f"Showing a sample of {MAX_VISIBLE_MARKERS} properties for better performance. Filter further to see more specific results.")
            st.write(f"Total matching properties: {len(filtered_data)}")
        else:
            display_data = filtered_data
            st.write(f"Showing {len(filtered_data)} properties")
        
        # Create layout with map on top and statistics below
        st.title("RealtyLens Property Analytics")

        # Display the map
        if filtered_data is not None and not filtered_data.empty:
            st.subheader("Property Map")
            
            # Show zoning option only for sales properties if available
            show_zoning = st.session_state.listing_type == "sale" and 'ZONING_CODE' in filtered_data.columns
            if show_zoning:
                show_zoning_toggle = st.checkbox("Show Zoning Areas", value=False)
            else:
                show_zoning_toggle = False
            
            # Get property map
            property_map = create_property_map(filtered_data, st.session_state.listing_type, show_zoning_toggle)
            
            # Display the map
            folium_static(property_map, width=1200)
            
            # Now place statistics directly below the map
            st.markdown("### Market Statistics")
            display_property_statistics_main(filtered_data, st.session_state.listing_type)
        else:
            st.info("No properties found matching your criteria. Try adjusting your filters.")
        
        # Continue with the rest of the app (property details or other content would go below)
        # ... existing code ...
    else:
        st.error("No data available. Please check your database connection.")
    
    # Always render database indicator at the very end
    render_db_indicator()

# Initialize all session state variables properly at the start of the app
def initialize_session_state():
    """Initialize all session state variables to prevent type errors"""
    # Define all variables that should be lists
    list_variables = ['property_history', 'search_history', 'viewed_properties']
    for var in list_variables:
        if var not in st.session_state:
            st.session_state[var] = []
        elif not isinstance(st.session_state[var], list):
            st.session_state[var] = []
    
    # Define variables that should be dictionaries
    dict_variables = ['property_stats', 'market_data']
    for var in dict_variables:
        if var not in st.session_state:
            st.session_state[var] = {}
        elif not isinstance(st.session_state[var], dict):
            st.session_state[var] = {}
    
    # Define all scalar variables with their default types
    scalar_defaults = {
        'db_hit_indicator': False,
        'query_count': 0,
        'filter_applied': False,
        'current_zip': "",
        'listing_type': "sale",
        'selected_property': None,
        'map_center': None,
        'snowflake_queries': []  # This should actually be a list
    }
    
    for var, default in scalar_defaults.items():
        if var not in st.session_state:
            st.session_state[var] = default
        elif not isinstance(st.session_state[var], type(default)) and var != 'selected_property':
            # Reset to default if type doesn't match (except for selected_property which can be None or dict)
            st.session_state[var] = default

# Call this function at the very beginning of your app
initialize_session_state()

# Make sure all session state variables are fixed whenever the app runs
if 'session_fixed' not in st.session_state:
    fix_all_session_state()
    st.session_state.session_fixed = True

# Function to create sample rental data
def create_sample_rental_data():
    """Create sample rental property data"""
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Create sample data with 20 properties
    n_samples = 20
    
    # Common address parts
    streets = ['Main St', 'Oak Ave', 'Maple Dr', 'Washington Blvd', 'Park Rd']
    cities = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose', 'Palo Alto']
    zip_codes = ['94101', '94102', '94103', '94104', '94105', '94606', '94607', '94704', '94705']
    
    # Generate random data
    data = {
        'LISTING_ID': [f"R{1000 + i}" for i in range(n_samples)],
        'PROPERTY_SK': [f"P{2000 + i}" for i in range(n_samples)],
        'PRICE': np.random.randint(1500, 5000, n_samples),  # Rent prices
        'PROPERTY_TYPE': np.random.choice(['Apartment', 'Condo', 'House', 'Townhouse'], n_samples),
        'BEDROOMS': np.random.choice([1, 2, 3, 4], n_samples),
        'BATHROOMS': np.random.choice([1, 1.5, 2, 2.5], n_samples),
        'SQUARE_FOOTAGE': np.random.randint(600, 2000, n_samples),
        'YEAR_BUILT': np.random.randint(1950, 2020, n_samples),
        'DAYS_ON_MARKET': np.random.randint(1, 60, n_samples),
        'ZIP_CODE': np.random.choice(zip_codes, n_samples),
        'ZONING_GROUP': np.random.choice(['Residential', 'Mixed-Use', 'Commercial'], n_samples),
        'ZONING_CODE': np.random.choice(['R-1', 'R-2', 'RM-1', 'C-1', 'M-1'], n_samples),
    }
    
    # Generate addresses
    addresses = []
    for i in range(n_samples):
        number = np.random.randint(100, 999)
        street = np.random.choice(streets)
        city = np.random.choice(cities)
        zip_code = data['ZIP_CODE'][i]
        addresses.append(f"{number} {street}, {city}, CA {zip_code}")
    
    data['FORMATTED_ADDRESS'] = addresses
    data['ADDRESS_LINE_1'] = [addr.split(',')[0] for addr in addresses]
    
    # Generate locations (San Francisco Bay Area)
    data['LATITUDE'] = np.random.uniform(37.7, 37.9, n_samples)
    data['LONGITUDE'] = np.random.uniform(-122.5, -122.3, n_samples)
    
    # Create load date
    data['LOAD_DATE'] = datetime.now().strftime('%Y-%m-%d')
    
    return pd.DataFrame(data)

# Function to create sample sales data
def create_sample_sales_data():
    """Create sample sales property data"""
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Create sample data with 20 properties
    n_samples = 20
    
    # Common address parts
    streets = ['Mission St', 'Valencia St', 'Castro St', 'Market St', 'Geary Blvd']
    cities = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose', 'Palo Alto']
    zip_codes = ['94101', '94102', '94103', '94104', '94105', '94606', '94607', '94704', '94705']
    
    # Generate random data
    data = {
        'LISTING_ID': [f"S{3000 + i}" for i in range(n_samples)],
        'PROPERTY_SK': [f"P{4000 + i}" for i in range(n_samples)],
        'PRICE': np.random.randint(500000, 2500000, n_samples),  # Sale prices
        'PROPERTY_TYPE': np.random.choice(['Single Family', 'Condo', 'Multi-Family', 'Townhouse'], n_samples),
        'BEDROOMS': np.random.choice([2, 3, 4, 5], n_samples),
        'BATHROOMS': np.random.choice([1, 1.5, 2, 2.5, 3], n_samples),
        'SQUARE_FOOTAGE': np.random.randint(800, 3000, n_samples),
        'YEAR_BUILT': np.random.randint(1920, 2020, n_samples),
        'DAYS_ON_MARKET': np.random.randint(5, 120, n_samples),
        'ZIP_CODE': np.random.choice(zip_codes, n_samples),
        'ZONING_GROUP': np.random.choice(['Residential', 'Mixed-Use', 'Commercial'], n_samples),
        'ZONING_CODE': np.random.choice(['R-1', 'R-2', 'RM-1', 'C-1', 'M-1'], n_samples),
    }
    
    # Generate addresses
    addresses = []
    for i in range(n_samples):
        number = np.random.randint(100, 999)
        street = np.random.choice(streets)
        city = np.random.choice(cities)
        zip_code = data['ZIP_CODE'][i]
        addresses.append(f"{number} {street}, {city}, CA {zip_code}")
    
    data['FORMATTED_ADDRESS'] = addresses
    data['ADDRESS_LINE_1'] = [addr.split(',')[0] for addr in addresses]
    
    # Generate locations (San Francisco Bay Area)
    data['LATITUDE'] = np.random.uniform(37.7, 37.9, n_samples)
    data['LONGITUDE'] = np.random.uniform(-122.5, -122.3, n_samples)
    
    # Additional sale-specific fields
    data['LAST_SALE_PRICE'] = [price * 0.8 for price in data['PRICE']]  # 80% of current price
    data['LAST_SALE_DATE'] = [(datetime.now() - pd.Timedelta(days=np.random.randint(365, 1825))).strftime('%Y-%m-%d') for _ in range(n_samples)]
    data['LOT_SIZE'] = np.random.randint(1000, 6000, n_samples)
    
    # Create load date
    data['LOAD_DATE'] = datetime.now().strftime('%Y-%m-%d')
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    main()
