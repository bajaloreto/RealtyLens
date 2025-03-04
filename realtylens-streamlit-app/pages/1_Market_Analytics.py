# Market Analytics Page for RealtyLens
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import altair as alt

# Import core functions from main app
from Property_Map import query_snowflake, get_snowflake_connection, render_db_indicator

# Page title
st.title("📊 RealtyLens Market Insights")
st.markdown("### Advanced Market Analytics for Property Investment Decisions")

# Sidebar filters
st.sidebar.header("Filter Options")

# Market type selection
market_type = st.sidebar.radio(
    "Market Type",
    ["Rental Market", "Sales Market"]
)

# Date range filter
today = datetime.datetime.now().date()
one_year_ago = today - datetime.timedelta(days=365)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(one_year_ago, today)
)

# ZIP code filter (optional)
zip_code = st.sidebar.text_input("ZIP Code (Optional)", "")

# Define any missing functions
def get_snowflake_connection():
    # Implementation here
    pass

def render_db_indicator():
    # Implementation here
    pass

# ======= HELPER FUNCTIONS =======
def format_price(price, currency="$"):
    """Format price with commas and currency symbol"""
    if pd.isna(price):
        return "N/A"
    try:
        return f"{currency}{int(float(price)):,}"
    except:
        return f"{currency}{price}"

def format_percent(value):
    """Format a decimal as a percentage"""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value) * 100:.1f}%"
    except:
        return f"{value}%"

# ======= MARKET ANALYTICS DATA LOADING =======
@st.cache_data(ttl=3600)
def load_market_health_data(listing_type="rent"):
    """Load market health data for rentals or sales"""
    if listing_type == "rent":
        query = """
        SELECT *
        FROM DATAEXPERT_STUDENT.JMUSNI07.RENT_MARKET_HEALTH_INDEX
        ORDER BY DAY
        """
    else:
        # For sales, we'll use the SALE_MARKET_TIMING_AND_SEASONALITY table
        query = """
        SELECT
            YEAR_MONTH as DAY,
            SUM(ACTIVE_LISTINGS) as TOTAL_LISTINGS,
            SUM(NEW_LISTINGS) as NEW_LISTINGS,
            SUM(LIKELY_SOLD) as LIKELY_SOLD,
            AVG(AVG_LIST_PRICE) as AVG_PRICE,
            AVG(AVG_DOM) as AVG_DAYS_ON_MARKET,
            AVG(SEASONALITY_INDEX) as SEASONALITY_INDEX,
            AVG(MARKET_VELOCITY) as MARKET_VELOCITY,
            AVG(MONTHS_OF_INVENTORY) as MONTHS_OF_INVENTORY,
            AVG(DISCOUNT_PRESSURE) as DISCOUNT_PRESSURE
        FROM DATAEXPERT_STUDENT.JMUSNI07.SALE_MARKET_TIMING_AND_SEASONALITY
        GROUP BY YEAR_MONTH
        ORDER BY YEAR_MONTH
        """
    
    data = query_snowflake(query)
    return data

@st.cache_data(ttl=3600)
def load_price_market_analysis(listing_type="rent"):
    """Load price market analysis data for rentals or sales"""
    if listing_type == "rent":
        table_name = "RENT_PRICE_MARKET_ANALYSIS"
        price_col = "AVG_RENT_PRICE"
    else:
        table_name = "SALE_PRICE_MARKET_ANALYSIS"
        price_col = "AVG_SALE_PRICE"
    
    query = f"""
    SELECT
        AGGREGATION_LEVEL,
        PROPERTY_TYPE,
        STATUS,
        BEDROOMS,
        LISTING_COUNT,
        {price_col} as AVG_PRICE,
        MIN_{listing_type.upper()}_PRICE as MIN_PRICE,
        MAX_{listing_type.upper()}_PRICE as MAX_PRICE
    FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name}
    ORDER BY LISTING_COUNT DESC
    """
    
    data = query_snowflake(query)
    return data

@st.cache_data(ttl=3600)
def load_lifecycle_data():
    """Load rental lifecycle data"""
    query = """
    SELECT *
    FROM DATAEXPERT_STUDENT.JMUSNI07.RENT_LIFECYCLE
    ORDER BY SNAPSHOT_DATE DESC
    """
    
    data = query_snowflake(query)
    return data

@st.cache_data(ttl=3600)
def load_price_optimization_data(listing_type="rent"):
    """Load price optimization data"""
    if listing_type == "rent":
        query = """
        SELECT *
        FROM DATAEXPERT_STUDENT.JMUSNI07.RENT_PRICE_OPTIMIZATION
        ORDER BY SNAPSHOT_DATE DESC, PRICE_QUINTILE
        """
    else:
        query = """
        SELECT 
            PRICE_SEGMENT,
            DAYS_SEGMENT,
            LISTING_COUNT,
            LIKELY_SOLD_COUNT,
            CONVERSION_RATE,
            AVG_PRICE_ADJUSTMENT_PCT,
            AVG_DAYS_TO_SELL,
            MARKET_EFFICIENCY_SCORE,
            SNAPSHOT_DATE
        FROM DATAEXPERT_STUDENT.JMUSNI07.SALE_PRICE_ELASTICITY_AND_DISCOUNT_IMPACT
        ORDER BY SNAPSHOT_DATE DESC, PRICE_SEGMENT
        """
    
    data = query_snowflake(query)
    return data

@st.cache_data(ttl=3600)
def load_seasonality_data():
    """Load sales seasonality data"""
    query = """
    SELECT *
    FROM DATAEXPERT_STUDENT.JMUSNI07.SALE_MARKET_TIMING_AND_SEASONALITY
    ORDER BY YEAR_MONTH
    """
    
    data = query_snowflake(query)
    return data

# ======= VISUALIZATION FUNCTIONS =======
def plot_market_health_trends(df, listing_type="rent"):
    """Plot market health trends for the given dataframe"""
    if df.empty:
        st.warning("No market trend data available for the selected filters.")
        return
    
    # First, check what columns we actually have
    available_cols = df.columns.tolist()
    st.write(f"Available columns: {available_cols}")  # Debug info - remove later
    
    # Determine date column based on available columns
    date_cols = [col for col in ['DAY', 'YEAR_MONTH', 'SNAPSHOT_DATE', 'DATE'] if col in available_cols]
    if not date_cols:
        st.error("No date column found in the data. Cannot display trends.")
        return
    
    date_col = date_cols[0]  # Use the first available date column
    
    # Ensure date column is datetime
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Get the most recent data
    latest = df.iloc[-1]
    
    # Identify days on market column
    dom_cols = [col for col in ['AVG_DAYS_ON_MARKET', 'AVG_DOM', 'AVG_DAYS_TO_SELL', 'DOM'] if col in available_cols]
    if not dom_cols:
        st.warning("No days on market data available.")
        dom_col = None
    else:
        dom_col = dom_cols[0]
    
    # Calculate historical average if we have days on market data
    if dom_col and len(df) > 1:
        # Convert to float to avoid Decimal issues
        current_dom = float(latest[dom_col])
        historical_avg = float(df[dom_col].mean())
        
        # Calculate percentage difference
        if historical_avg > 0:
            pct_diff = ((current_dom - historical_avg) / historical_avg) * 100
        else:
            pct_diff = 0
    else:
        pct_diff = 0
        current_dom = 0
        historical_avg = 0
    
    # Create metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Market health or inventory metric
        if 'MARKET_HEALTH_SCORE' in available_cols:
            health_score = float(latest['MARKET_HEALTH_SCORE'])
            prev_score = float(df.iloc[-2]['MARKET_HEALTH_SCORE']) if len(df) > 1 else health_score
            delta = health_score - prev_score
            
            st.metric(
                "Market Health Score", 
                f"{health_score:.1f}/10", 
                f"{delta:+.1f}"
            )
        elif 'MONTHS_OF_INVENTORY' in available_cols:
            moi = float(latest['MONTHS_OF_INVENTORY'])
            prev_moi = float(df.iloc[-2]['MONTHS_OF_INVENTORY']) if len(df) > 1 else moi
            delta = moi - prev_moi
            
            st.metric(
                "Months of Inventory", 
                f"{moi:.1f}", 
                f"{delta:+.1f}",
                delta_color="inverse"  # Lower is better for sellers
            )
        elif 'MARKET_VELOCITY' in available_cols:
            velocity = float(latest['MARKET_VELOCITY'])
            prev_velocity = float(df.iloc[-2]['MARKET_VELOCITY']) if len(df) > 1 else velocity
            delta = velocity - prev_velocity
            
            st.metric(
                "Market Velocity", 
                f"{velocity:.2f}", 
                f"{delta:+.2f}"
            )
        else:
            st.metric("Market Status", "Data Available", "")
    
    with col2:
        # Days on market
        if dom_col:
            st.metric(
                "Days on Market", 
                f"{current_dom:.1f}", 
                f"{pct_diff:+.1f}% vs avg",
                delta_color="inverse"  # Lower is better
            )
        else:
            st.metric("Days on Market", "N/A", "")
    
    with col3:
        # Price metric - check for available price columns
        price_cols = [col for col in ['AVG_PRICE', 'MEDIAN_PRICE', 'AVG_SALE_PRICE', 'AVG_LIST_PRICE'] 
                     if col in available_cols]
        
        if price_cols:
            price_col = price_cols[0]
            current_price = float(latest[price_col])
            prev_price = float(df.iloc[-2][price_col]) if len(df) > 1 else current_price
            price_delta = ((current_price - prev_price) / prev_price * 100) if prev_price > 0 else 0
            
            price_label = "Avg Price" if "AVG" in price_col else "Median Price"
            
            st.metric(
                price_label, 
                f"${current_price:,.0f}", 
                f"{price_delta:+.1f}%"
            )
        else:
            st.metric("Price", "N/A", "")
    
    # Plot time series if we have enough data
    if len(df) > 1:
        st.subheader("Market Trends")
        
        # Create tabs for different metrics
        tabs = []
        tab_titles = []
        
        # First tab - always have health or inventory
        if 'MARKET_HEALTH_SCORE' in available_cols:
            tabs.append("Health Score")
            tab_titles.append("Health Score")
        elif 'MONTHS_OF_INVENTORY' in available_cols:
            tabs.append("Inventory")
            tab_titles.append("Inventory")
        elif 'MARKET_VELOCITY' in available_cols:
            tabs.append("Velocity")
            tab_titles.append("Velocity")
        
        # Second tab - Days on Market
        if dom_col:
            tabs.append("Days on Market")
            tab_titles.append("Days")
        
        # Third tab - Price Trends
        if price_cols:
            tabs.append("Price Trends")
            tab_titles.append("Prices")
        
        # Add listing activity if available
        if any(col in available_cols for col in ['NEW_LISTINGS', 'TOTAL_LISTINGS', 'ACTIVE_LISTINGS']):
            tabs.append("Listings")
            tab_titles.append("Listings")
        
        # Create the tabs
        if tabs:
            plot_tabs = st.tabs(tabs)
            
            # Fill the tabs with plots
            for i, tab_name in enumerate(tab_titles):
                with plot_tabs[i]:
                    if tab_name == "Health Score" and 'MARKET_HEALTH_SCORE' in available_cols:
                        plot_health_score(df, date_col)
                    elif tab_name == "Inventory" and 'MONTHS_OF_INVENTORY' in available_cols:
                        plot_inventory_trends(df, date_col)
                    elif tab_name == "Velocity" and 'MARKET_VELOCITY' in available_cols:
                        plot_velocity_trends(df, date_col)
                    elif tab_name == "Days" and dom_col:
                        plot_days_on_market(df, date_col, dom_col)
                    elif tab_name == "Prices" and price_cols:
                        plot_price_trends(df, date_col, price_cols[0])
                    elif tab_name == "Listings":
                        plot_listing_activity(df, date_col)
        else:
            st.info("Not enough data to display trend charts.")

# Add these helper functions for the plot_market_health_trends function

def plot_health_score(df, date_col):
    """Plot the market health score trend"""
    fig = go.Figure()
    
    # Add market health score line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df['MARKET_HEALTH_SCORE'].apply(lambda x: float(x)),
        mode='lines+markers',
        name='Market Health Score',
        line=dict(color='#4CAF50', width=3),
        hovertemplate='Date: %{x|%b %d, %Y}<br>Health Score: %{y:.1f}/10<extra></extra>'
    ))
    
    # Add reference line at 5.0 (neutral market)
    fig.add_shape(
        type="line",
        x0=df[date_col].min(),
        x1=df[date_col].max(),
        y0=5.0,
        y1=5.0,
        line=dict(color="gray", width=1, dash="dash")
    )
    
    # Add annotations for market conditions
    fig.add_annotation(
        x=df[date_col].max(),
        y=8.5,
        text="Landlord's Market",
        showarrow=False,
        font=dict(color="#4CAF50")
    )
    
    fig.add_annotation(
        x=df[date_col].max(),
        y=1.5,
        text="Renter's Market",
        showarrow=False,
        font=dict(color="#F44336")
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Market Health Score (0-10)",
        yaxis=dict(range=[0, 10]),
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_inventory_trends(df, date_col):
    """Plot months of inventory trend"""
    fig = go.Figure()
    
    # Add inventory line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df['MONTHS_OF_INVENTORY'].apply(lambda x: float(x)),
        mode='lines+markers',
        name='Months of Inventory',
        line=dict(color='#2196F3', width=3),
        hovertemplate='Date: %{x|%b %d, %Y}<br>Months of Inventory: %{y:.1f}<extra></extra>'
    ))
    
    # Add reference lines for market conditions
    fig.add_shape(
        type="line",
        x0=df[date_col].min(),
        x1=df[date_col].max(),
        y0=3.0,
        y1=3.0,
        line=dict(color="#4CAF50", width=1, dash="dash")
    )
    
    fig.add_shape(
        type="line",
        x0=df[date_col].min(),
        x1=df[date_col].max(),
        y0=6.0,
        y1=6.0,
        line=dict(color="#F44336", width=1, dash="dash")
    )
    
    # Add annotations for market conditions
    fig.add_annotation(
        x=df[date_col].max(),
        y=1.5,
        text="Seller's Market",
        showarrow=False,
        font=dict(color="#4CAF50")
    )
    
    fig.add_annotation(
        x=df[date_col].max(),
        y=4.5,
        text="Balanced Market",
        showarrow=False,
        font=dict(color="#FFB300")
    )
    
    fig.add_annotation(
        x=df[date_col].max(),
        y=7.5,
        text="Buyer's Market",
        showarrow=False,
        font=dict(color="#F44336")
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Months of Inventory",
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_velocity_trends(df, date_col):
    """Plot market velocity trend"""
    fig = go.Figure()
    
    # Add velocity line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df['MARKET_VELOCITY'].apply(lambda x: float(x)),
        mode='lines+markers',
        name='Market Velocity',
        line=dict(color='#673AB7', width=3),
        hovertemplate='Date: %{x|%b %d, %Y}<br>Market Velocity: %{y:.2f}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Market Velocity",
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_days_on_market(df, date_col, dom_col):
    """Plot days on market trend"""
    fig = go.Figure()
    
    # Add days on market line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[dom_col].apply(lambda x: float(x)),
        mode='lines+markers',
        name='Days on Market',
        line=dict(color='#FF9800', width=3),
        hovertemplate='Date: %{x|%b %d, %Y}<br>Days on Market: %{y:.1f}<extra></extra>'
    ))
    
    # Add historical average reference line
    historical_avg = float(df[dom_col].mean())
    
    fig.add_shape(
        type="line",
        x0=df[date_col].min(),
        x1=df[date_col].max(),
        y0=historical_avg,
        y1=historical_avg,
        line=dict(color="gray", width=1, dash="dash")
    )
    
    fig.add_annotation(
        x=df[date_col].max(),
        y=historical_avg,
        text=f"Historical Avg: {historical_avg:.1f}",
        showarrow=False,
        yshift=10,
        font=dict(color="gray")
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Days on Market",
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_price_trends(df, date_col, price_col):
    """Plot price trend"""
    fig = go.Figure()
    
    # Add price line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[price_col].apply(lambda x: float(x)),
        mode='lines+markers',
        name=price_col.replace('_', ' ').title(),
        line=dict(color='#E91E63', width=3),
        hovertemplate='Date: %{x|%b %d, %Y}<br>Price: $%{y:,.0f}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Price ($)",
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_listing_activity(df, date_col):
    """Plot listing activity"""
    fig = go.Figure()
    
    # Determine which columns we have
    cols = df.columns
    
    if 'TOTAL_LISTINGS' in cols or 'ACTIVE_LISTINGS' in cols:
        total_col = 'TOTAL_LISTINGS' if 'TOTAL_LISTINGS' in cols else 'ACTIVE_LISTINGS'
        fig.add_trace(go.Scatter(
            x=df[date_col],
            y=df[total_col].apply(lambda x: float(x)),
            mode='lines',
            name='Total Listings',
            line=dict(color='#2196F3', width=2)
        ))
    
    if 'NEW_LISTINGS' in cols:
        fig.add_trace(go.Bar(
            x=df[date_col],
            y=df['NEW_LISTINGS'].apply(lambda x: float(x)),
            name='New Listings',
            marker_color='#4CAF50'
        ))
    
    if 'CHURNED_LISTINGS' in cols:
        fig.add_trace(go.Bar(
            x=df[date_col],
            y=df['CHURNED_LISTINGS'].apply(lambda x: float(x)),
            name='Churned Listings',
            marker_color='#F44336'
        ))
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Listings",
        height=400,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def display_price_analysis(price_data, listing_type="rent"):
    """Display price analysis by property type, bedrooms, etc."""
    if price_data.empty:
        st.warning("No price analysis data available")
        return
    
    st.subheader(f"{'Rental' if listing_type=='rent' else 'Sales'} Price Analysis")
    
    # Create a filter for aggregation level
    aggregation_levels = price_data['AGGREGATION_LEVEL'].unique().tolist()
    selected_agg = st.selectbox(
        "Segmentation Level",
        options=aggregation_levels,
        index=0 if 'PROPERTY_TYPE' in aggregation_levels else 0
    )
    
    # Filter data by selected aggregation level
    filtered_data = price_data[price_data['AGGREGATION_LEVEL'] == selected_agg]
    
    # Create visualization based on the aggregation type
    if selected_agg == 'PROPERTY_TYPE':
        # Bar chart by property type
        if 'PROPERTY_TYPE' in filtered_data.columns:
            fig = px.bar(
                filtered_data,
                x='PROPERTY_TYPE',
                y='AVG_PRICE',
                color='PROPERTY_TYPE',
                title=f"Average Price by Property Type",
                labels={'AVG_PRICE': 'Average Price ($)', 'PROPERTY_TYPE': 'Property Type'},
                hover_data=['LISTING_COUNT', 'MIN_PRICE', 'MAX_PRICE']
            )
            
            fig.update_layout(
                xaxis_title='',
                yaxis_title='Average Price ($)',
                showlegend=False,
                margin=dict(l=40, r=40, t=60, b=40),
                height=500
            )
            
            # Add price range as error bars
            fig.update_traces(
                error_y=dict(
                    type='data',
                    symmetric=False,
                    array=filtered_data['MAX_PRICE'] - filtered_data['AVG_PRICE'],
                    arrayminus=filtered_data['AVG_PRICE'] - filtered_data['MIN_PRICE']
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show data table with key metrics
            display_df = filtered_data[['PROPERTY_TYPE', 'LISTING_COUNT', 'AVG_PRICE', 'MIN_PRICE', 'MAX_PRICE']].copy()
            display_df['AVG_PRICE'] = display_df['AVG_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MIN_PRICE'] = display_df['MIN_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MAX_PRICE'] = display_df['MAX_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df.columns = ['Property Type', 'Listing Count', 'Average Price', 'Minimum Price', 'Maximum Price']
            
            st.dataframe(display_df, use_container_width=True)
    
    elif selected_agg == 'BEDROOMS':
        # Line chart by number of bedrooms
        if 'BEDROOMS' in filtered_data.columns:
            # Ensure proper sorting order by converting to numeric
            filtered_data['BEDROOMS_NUM'] = pd.to_numeric(filtered_data['BEDROOMS'], errors='coerce')
            filtered_data = filtered_data.sort_values('BEDROOMS_NUM')
            
            fig = px.line(
                filtered_data,
                x='BEDROOMS',
                y='AVG_PRICE',
                markers=True,
                title=f"Average Price by Number of Bedrooms",
                labels={'AVG_PRICE': 'Average Price ($)', 'BEDROOMS': 'Bedrooms'},
                line_shape='linear',
                color_discrete_sequence=['#1E88E5']
            )
            
            # Add price range as a shaded area
            fig.add_trace(
                go.Scatter(
                    x=filtered_data['BEDROOMS'],
                    y=filtered_data['MAX_PRICE'],
                    mode='lines',
                    line=dict(width=0),
                    showlegend=False
                )
            )
            
            fig.add_trace(
                go.Scatter(
                    x=filtered_data['BEDROOMS'],
                    y=filtered_data['MIN_PRICE'],
                    mode='lines',
                    line=dict(width=0),
                    fill='tonexty',
                    fillcolor='rgba(30, 136, 229, 0.2)',
                    showlegend=False
                )
            )
            
            fig.update_layout(
                xaxis_title='Number of Bedrooms',
                yaxis_title='Average Price ($)',
                margin=dict(l=40, r=40, t=60, b=40),
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display data table
            display_df = filtered_data[['BEDROOMS', 'LISTING_COUNT', 'AVG_PRICE', 'MIN_PRICE', 'MAX_PRICE']].copy()
            display_df['AVG_PRICE'] = display_df['AVG_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MIN_PRICE'] = display_df['MIN_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MAX_PRICE'] = display_df['MAX_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df.columns = ['Bedrooms', 'Listing Count', 'Average Price', 'Minimum Price', 'Maximum Price']
            
            st.dataframe(display_df, use_container_width=True)
    
    elif selected_agg == 'STATUS':
        # Bar chart by property status
        if 'STATUS' in filtered_data.columns:
            fig = px.bar(
                filtered_data,
                x='STATUS',
                y='AVG_PRICE',
                color='STATUS',
                title=f"Average Price by Property Status",
                labels={'AVG_PRICE': 'Average Price ($)', 'STATUS': 'Status'},
                hover_data=['LISTING_COUNT', 'MIN_PRICE', 'MAX_PRICE']
            )
            
            fig.update_layout(
                xaxis_title='',
                yaxis_title='Average Price ($)',
                showlegend=False,
                margin=dict(l=40, r=40, t=60, b=40),
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display data table
            display_df = filtered_data[['STATUS', 'LISTING_COUNT', 'AVG_PRICE', 'MIN_PRICE', 'MAX_PRICE']].copy()
            display_df['AVG_PRICE'] = display_df['AVG_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MIN_PRICE'] = display_df['MIN_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df['MAX_PRICE'] = display_df['MAX_PRICE'].apply(lambda x: f"${x:,.0f}")
            display_df.columns = ['Status', 'Listing Count', 'Average Price', 'Minimum Price', 'Maximum Price']
            
            st.dataframe(display_df, use_container_width=True)
    
    # Add a note about the data
    st.caption(f"Data shows average prices across {len(filtered_data)} different segments.")

def display_rental_lifecycle(lifecycle_data):
    """Display rental property lifecycle analysis"""
    if lifecycle_data.empty:
        st.warning("No rental lifecycle data available")
        return
    
    st.subheader("Rental Property Lifecycle Analysis")
    
    # Get the most recent snapshot date
    latest_date = lifecycle_data['SNAPSHOT_DATE'].max()
    latest_data = lifecycle_data[lifecycle_data['SNAPSHOT_DATE'] == latest_date]
    
    # Create a funnel chart for the lifecycle stages
    stages = ['NEW', 'ACTIVE', 'PENDING', 'RENTED']
    stage_data = []
    
    for stage in stages:
        stage_row = latest_data[latest_data['LIFECYCLE_STAGE'] == stage]
        if not stage_row.empty:
            stage_data.append(stage_row['PROPERTY_COUNT'].iloc[0])
        else:
            stage_data.append(0)
    
    fig = go.Figure(go.Funnel(
        y=stages,
        x=stage_data,
        textinfo="value+percent initial",
        marker={"color": ["#4CAF50", "#2196F3", "#FF9800", "#9C27B0"]}
    ))
    
    fig.update_layout(
        title={
            'text': "Rental Property Funnel",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        margin=dict(l=40, r=40, t=80, b=40),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Create metrics for key lifecycle statistics
    st.subheader("Lifecycle Metrics")
    
    # Create a 3-column layout
    col1, col2, col3 = st.columns(3)
    
    # Conversion rate
    with col1:
        # Find the conversion rate from NEW to RENTED
        conversion_data = latest_data[latest_data['LIFECYCLE_STAGE'].isin(['RENTED'])]
        if not conversion_data.empty:
            conversion_rate = conversion_data['CONVERSION_RATE'].iloc[0]
            st.metric("Overall Conversion Rate", f"{conversion_rate * 100:.1f}%")
            
            # Interpretation
            if conversion_rate > 0.6:
                st.write("High conversion rate indicates strong market demand")
            elif conversion_rate < 0.4:
                st.write("Low conversion rate suggests potential pricing issues")
            else:
                st.write("Average conversion rate for the current market")
    
    # Price drop rate
    with col2:
        price_drop_data = latest_data[latest_data['LIFECYCLE_STAGE'].isin(['ACTIVE', 'PENDING'])]
        if not price_drop_data.empty:
            avg_price_drop = price_drop_data['PRICE_DROP_RATE'].mean()
            st.metric("Price Drop Rate", f"{avg_price_drop * 100:.1f}%")
            
            # Interpretation
            if avg_price_drop > 0.3:
                st.write("High price drop rate indicates initial overpricing")
            elif avg_price_drop < 0.1:
                st.write("Low price drop rate suggests accurate initial pricing")
            else:
                st.write("Moderate price adjustments in line with market expectations")
    
    # Days to conversion
    with col3:
        days_data = latest_data[latest_data['LIFECYCLE_STAGE'].isin(['RENTED'])]
        if not days_data.empty:
            avg_days = days_data['AVG_DAYS_TO_CONVERSION'].iloc[0]
            st.metric("Avg Days to Rent", f"{avg_days:.1f} days")
            
            # Interpretation
            if avg_days < 14:
                st.write("Properties are renting quickly in this market")
            elif avg_days > 30:
                st.write("Longer than average time to rent")
            else:
                st.write("Average rental timeline for the current market")
    
    # Show the data table with all lifecycle statistics
    with st.expander("View Detailed Lifecycle Statistics"):
        display_df = latest_data.copy()
        display_df['CONVERSION_RATE'] = display_df['CONVERSION_RATE'].apply(lambda x: f"{x * 100:.1f}%")
        display_df['PRICE_DROP_RATE'] = display_df['PRICE_DROP_RATE'].apply(lambda x: f"{x * 100:.1f}%")
        display_df['AVG_DAYS_TO_CONVERSION'] = display_df['AVG_DAYS_TO_CONVERSION'].apply(lambda x: f"{x:.1f} days")
        display_df.columns = [col.replace('_', ' ').title() for col in display_df.columns]
        
        st.dataframe(display_df, use_container_width=True)

def display_price_optimization(opt_data, listing_type="rent"):
    """Display price optimization insights"""
    if opt_data.empty:
        st.warning("No price optimization data available")
        return
    
    st.subheader(f"{'Rental' if listing_type=='rent' else 'Sales'} Price Optimization")
    
    if listing_type == "rent":
        # For rentals, use the RENT_PRICE_OPTIMIZATION table
        
        # Get the most recent snapshot date
        latest_date = opt_data['SNAPSHOT_DATE'].max()
        latest_data = opt_data[opt_data['SNAPSHOT_DATE'] == latest_date]
        
        # Create a plot showing conversion rate by price quintile
        fig = px.bar(
            latest_data,
            x='PRICE_QUINTILE',
            y='CONVERSION_RATE',
            color='PRICE_STRATEGY',
            title="Conversion Rate by Price Quintile and Strategy",
            labels={
                'PRICE_QUINTILE': 'Price Quintile (1=Lowest, 5=Highest)',
                'CONVERSION_RATE': 'Conversion Rate',
                'PRICE_STRATEGY': 'Price Strategy'
            },
            color_discrete_map={
                'Underpriced': '#4CAF50',
                'Optimal': '#2196F3',
                'Overpriced': '#F44336'
            },
            barmode='group'
        )
        
        fig.update_layout(
            xaxis_title='Price Quintile',
            yaxis_title='Conversion Rate',
            yaxis_tickformat='.0%',
            margin=dict(l=40, r=40, t=60, b=40),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Create a second chart showing days on market by price quintile
        fig2 = px.line(
            latest_data,
            x='PRICE_QUINTILE',
            y='AVG_DAYS_ON_MARKET',
            color='PRICE_STRATEGY',
            title="Days on Market by Price Quintile and Strategy",
            labels={
                'PRICE_QUINTILE': 'Price Quintile (1=Lowest, 5=Highest)',
                'AVG_DAYS_ON_MARKET': 'Average Days on Market',
                'PRICE_STRATEGY': 'Price Strategy'
            },
            color_discrete_map={
                'Underpriced': '#4CAF50',
                'Optimal': '#2196F3',
                'Overpriced': '#F44336'
            },
            markers=True
        )
        
        fig2.update_layout(
            xaxis_title='Price Quintile',
            yaxis_title='Average Days on Market',
            margin=dict(l=40, r=40, t=60, b=40),
            height=400
        )
        
        st.plotly_chart(fig2, use_container_width=True)
        
        # Key insights
        st.subheader("Pricing Strategy Insights")
        
        # Calculate average metrics for each strategy
        strategy_metrics = latest_data.groupby('PRICE_STRATEGY').agg({
            'CONVERSION_RATE': 'mean',
            'AVG_DAYS_ON_MARKET': 'mean',
            'AVG_PRICE_ADJUSTMENT_PCT': 'mean'
        }).reset_index()
        
        # Create metrics in 3 columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Find the strategy with highest conversion rate
            best_conv = strategy_metrics.loc[strategy_metrics['CONVERSION_RATE'].idxmax()]
            st.metric(
                "Best Strategy for Fast Rental", 
                best_conv['PRICE_STRATEGY'],
                f"{best_conv['CONVERSION_RATE']*100:.1f}% conversion rate"
            )
        
        with col2:
            # Find the strategy with lowest days on market
            best_dom = strategy_metrics.loc[strategy_metrics['AVG_DAYS_ON_MARKET'].idxmin()]
            st.metric(
                "Best Strategy for Minimum Time", 
                best_dom['PRICE_STRATEGY'],
                f"{best_dom['AVG_DAYS_ON_MARKET']:.1f} days on market"
            )
        
        with col3:
            # Find the strategy with lowest price adjustment
            best_adj = strategy_metrics.loc[strategy_metrics['AVG_PRICE_ADJUSTMENT_PCT'].idxmin()]
            st.metric(
                "Best Strategy for Price Stability", 
                best_adj['PRICE_STRATEGY'],
                f"{abs(best_adj['AVG_PRICE_ADJUSTMENT_PCT'])*100:.1f}% price adjustment"
            )
    
    else:
        # For sales, use the SALE_PRICE_ELASTICITY_AND_DISCOUNT_IMPACT table
        
        # Get the most recent snapshot date
        latest_date = opt_data['SNAPSHOT_DATE'].max()
        latest_data = opt_data[opt_data['SNAPSHOT_DATE'] == latest_date]
        
        # Create a heatmap showing conversion rate by price segment and days segment
        pivot_data = latest_data.pivot_table(
            values='CONVERSION_RATE',
            index='PRICE_SEGMENT',
            columns='DAYS_SEGMENT',
            aggfunc='mean'
        )
        
        fig = px.imshow(
            pivot_data,
            labels=dict(x="Days on Market Segment", y="Price Segment", color="Conversion Rate"),
            x=pivot_data.columns,
            y=pivot_data.index,
            color_continuous_scale="RdBu_r",
            title="Sales Conversion Rate by Price and Time on Market"
        )
        
        fig.update_layout(
            xaxis_title='Days on Market Segment',
            yaxis_title='Price Segment',
            margin=dict(l=40, r=40, t=60, b=40),
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Create a second chart showing price adjustment impact
        fig2 = px.scatter(
            latest_data,
            x='AVG_PRICE_ADJUSTMENT_PCT',
            y='CONVERSION_RATE',
            size='LISTING_COUNT',
            color='PRICE_SEGMENT',
            title="Impact of Price Adjustments on Sales Conversion Rate",
            labels={
                'AVG_PRICE_ADJUSTMENT_PCT': 'Average Price Adjustment %',
                'CONVERSION_RATE': 'Conversion Rate',
                'LISTING_COUNT': 'Number of Listings',
                'PRICE_SEGMENT': 'Price Segment'
            },
            hover_data=['DAYS_SEGMENT', 'AVG_DAYS_TO_SELL', 'MARKET_EFFICIENCY_SCORE']
        )
        
        fig2.update_layout(
            xaxis_title='Average Price Adjustment %',
            yaxis_title='Conversion Rate',
            xaxis_tickformat='.0%',
            yaxis_tickformat='.0%',
            margin=dict(l=40, r=40, t=60, b=40),
            height=500
        )
        
        # Add a vertical line at 0% price adjustment
        fig2.add_vline(x=0, line_dash="dash", line_color="gray")
        
        # Add annotations explaining the quadrants
        fig2.add_annotation(
            x=0.05, y=0.8,
            text="Price increases<br>with high conversion<br>(Strong market)",
            showarrow=False,
            font=dict(size=10)
        )
        
        fig2.add_annotation(
            x=-0.05, y=0.8,
            text="Price reductions<br>with high conversion<br>(Optimal pricing)",
            showarrow=False,
            font=dict(size=10)
        )
        
        fig2.add_annotation(
            x=0.05, y=0.2,
            text="Price increases<br>with low conversion<br>(Overpriced)",
            showarrow=False,
            font=dict(size=10)
        )
        
        fig2.add_annotation(
            x=-0.05, y=0.2,
            text="Price reductions<br>with low conversion<br>(Market resistance)",
            showarrow=False,
            font=dict(size=10)
        )
        
        st.plotly_chart(fig2, use_container_width=True)
        
        # Key insights
        st.subheader("Sales Price Optimization Insights")
        
        # Calculate optimal price segment and days segment
        optimal_segment = latest_data.loc[latest_data['MARKET_EFFICIENCY_SCORE'].idxmax()]
        
        # Create metrics in a row
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Most Efficient Price Segment", 
                optimal_segment['PRICE_SEGMENT'],
                f"Efficiency Score: {optimal_segment['MARKET_EFFICIENCY_SCORE']:.2f}"
            )
            
            # Add explanation
            st.write(f"This price segment has the best balance of conversion rate and time on market.")
        
        with col2:
            st.metric(
                "Optimal Days on Market Segment", 
                optimal_segment['DAYS_SEGMENT'],
                f"Avg Days to Sell: {optimal_segment['AVG_DAYS_TO_SELL']:.1f}"
            )
            
            # Add price adjustment recommendation
            adjustment = optimal_segment['AVG_PRICE_ADJUSTMENT_PCT']
            if adjustment < 0:
                st.write(f"Recommended price adjustment: **{abs(adjustment)*100:.1f}% reduction** for optimal results")
            else:
                st.write(f"This segment supports a **{adjustment*100:.1f}% price premium** in the current market")

def display_sales_seasonality(df):
    """Display sales seasonality analysis"""
    if df.empty:
        st.warning("No seasonality data available.")
        return
    
    st.subheader("Market Seasonality Analysis")
    
    # First, ensure YEAR_MONTH is datetime - this is critical
    if 'YEAR_MONTH' in df.columns:
        try:
            df['YEAR_MONTH'] = pd.to_datetime(df['YEAR_MONTH'])
        except Exception as e:
            st.error(f"Error converting YEAR_MONTH to datetime: {e}")
            st.write("Column data sample:", df['YEAR_MONTH'].head())
            return
    else:
        st.error("Required column 'YEAR_MONTH' not found in data.")
        st.write("Available columns:", df.columns.tolist())
        return
    
    # Get unique price segments
    if 'PRICE_SEGMENT' in df.columns:
        segments = sorted(df['PRICE_SEGMENT'].unique().tolist())
        
        # Create segment selection
        selected_segment = st.selectbox(
            "Select Price Segment",
            ["All Segments"] + segments
        )
        
        # Filter by segment
        if selected_segment != "All Segments":
            segment_data = df[df['PRICE_SEGMENT'] == selected_segment].copy()
        else:
            # We need to avoid using dt.to_period directly on the entire column
            # Instead, extract month information directly
            segment_data = df.copy()
            segment_data['MONTH'] = segment_data['YEAR_MONTH'].dt.month
            segment_data['MONTH_NAME'] = segment_data['YEAR_MONTH'].dt.strftime('%b')
            
            # Group by month
            monthly_agg = segment_data.groupby(['MONTH', 'MONTH_NAME']).agg({
                'ACTIVE_LISTINGS': 'mean',
                'NEW_LISTINGS': 'mean',
                'LIKELY_SOLD': 'mean',
                'AVG_LIST_PRICE': 'mean',
                'MEDIAN_PRICE': 'mean',
                'AVG_DOM': 'mean',
                'AVG_DAYS_TO_SELL': 'mean',
                'MARKET_VELOCITY': 'mean',
                'SEASONALITY_INDEX': 'mean'
            }).reset_index()
            
            # This is our aggregated data
            segment_data = monthly_agg
    else:
        # If no price segment, just add month columns
        segment_data = df.copy()
        segment_data['MONTH'] = segment_data['YEAR_MONTH'].dt.month
        segment_data['MONTH_NAME'] = segment_data['YEAR_MONTH'].dt.strftime('%b')
    
    # If we don't already have MONTH columns, create them now
    if 'MONTH' not in segment_data.columns:
        segment_data['MONTH'] = segment_data['YEAR_MONTH'].dt.month
        segment_data['MONTH_NAME'] = segment_data['YEAR_MONTH'].dt.strftime('%b')
    
    # Group by month if not already grouped
    if 'MONTH' in segment_data.columns and 'MONTH_NAME' in segment_data.columns:
        # Check if we need to group (i.e., if there are multiple rows per month)
        month_counts = segment_data['MONTH'].value_counts()
        if month_counts.max() > 1:
            monthly_data = segment_data.groupby(['MONTH', 'MONTH_NAME']).agg({
                'ACTIVE_LISTINGS': 'mean',
                'NEW_LISTINGS': 'mean',
                'LIKELY_SOLD': 'mean',
                'AVG_DAYS_TO_SELL': 'mean',
                'SEASONALITY_INDEX': 'mean'
            }).reset_index()
        else:
            # Already grouped, just use segment_data
            monthly_data = segment_data
    else:
        st.error("Unable to process month data.")
        return
    
    # Sort by month
    monthly_data = monthly_data.sort_values('MONTH')
    
    # Create seasonality chart
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Create bar chart for seasonality index
        fig = go.Figure()
        
        # Add seasonality index bars
        if 'SEASONALITY_INDEX' in monthly_data.columns:
            fig.add_trace(go.Bar(
                x=monthly_data['MONTH_NAME'],
                y=monthly_data['SEASONALITY_INDEX'].apply(lambda x: float(x)),
                marker_color=monthly_data['SEASONALITY_INDEX'].apply(
                    lambda x: '#4CAF50' if float(x) > 1 else '#F44336'),
                text=monthly_data['SEASONALITY_INDEX'].apply(lambda x: f"{float(x):.2f}"),
                textposition='auto',
                name='Seasonality Index'
            ))
            
            # Add reference line at 1.0 (average seasonality)
            fig.add_shape(
                type="line",
                x0=-0.5,
                x1=len(monthly_data)-0.5,
                y0=1.0,
                y1=1.0,
                line=dict(color="black", width=1, dash="dash")
            )
            
            # Update layout
            fig.update_layout(
                title="Market Seasonality by Month",
                xaxis_title="Month",
                yaxis_title="Seasonality Index (1.0 = Average)",
                height=400,
                margin=dict(l=40, r=40, t=60, b=40)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Seasonality index data not available.")
    
    with col2:
        # Find best and worst months
        if 'SEASONALITY_INDEX' in monthly_data.columns:
            # Fix: Convert to list and get index as integer
            seasonality_values = monthly_data['SEASONALITY_INDEX'].tolist()
            if seasonality_values:
                best_month_idx = seasonality_values.index(max(seasonality_values))
                worst_month_idx = seasonality_values.index(min(seasonality_values))
                
                # Now these are integer indices that can be used with iloc
                if 0 <= best_month_idx < len(monthly_data) and 0 <= worst_month_idx < len(monthly_data):
                    best_month = monthly_data.iloc[best_month_idx]
                    worst_month = monthly_data.iloc[worst_month_idx]
                    
                    st.subheader("Best Time to Sell")
                    st.markdown(f"""
                    **Best Month:** {best_month['MONTH_NAME']}
                    
                    **Seasonality Index:** {float(best_month['SEASONALITY_INDEX']):.2f}
                    
                    **Avg Days to Sell:** {float(best_month['AVG_DAYS_TO_SELL']) if 'AVG_DAYS_TO_SELL' in best_month else 'N/A'}
                    
                    **Likely Sold:** {float(best_month['LIKELY_SOLD']) if 'LIKELY_SOLD' in best_month else 'N/A'} properties
                    """)
                    
                    st.subheader("Slowest Market Period")
                    st.markdown(f"""
                    **Slowest Month:** {worst_month['MONTH_NAME']}
                    
                    **Seasonality Index:** {float(worst_month['SEASONALITY_INDEX']):.2f}
                    
                    **Avg Days to Sell:** {float(worst_month['AVG_DAYS_TO_SELL']) if 'AVG_DAYS_TO_SELL' in worst_month else 'N/A'}
                    
                    **Likely Sold:** {float(worst_month['LIKELY_SOLD']) if 'LIKELY_SOLD' in worst_month else 'N/A'} properties
                    """)
                else:
                    st.warning("Unable to determine best/worst months from the data.")
            else:
                st.warning("No seasonality data available to determine best/worst months.")
        else:
            st.warning("Seasonality index data not available.")
    
    # Additional metrics
    st.subheader("Monthly Market Metrics")
    
    # Check required columns are available before creating visualization
    required_cols = ['NEW_LISTINGS', 'LIKELY_SOLD', 'AVG_DAYS_TO_SELL', 'ACTIVE_LISTINGS']
    available_cols = [col for col in required_cols if col in monthly_data.columns]
    
    if len(available_cols) > 0:
        # Create metrics visualizations with available columns
        num_cols = len(available_cols)
        subplot_rows = (num_cols + 1) // 2  # Ceiling division for rows
        subplot_cols = min(2, num_cols)     # Maximum 2 columns
        
        titles = []
        for col in available_cols:
            if col == 'NEW_LISTINGS':
                titles.append("New Listings by Month")
            elif col == 'LIKELY_SOLD':
                titles.append("Properties Sold by Month")
            elif col == 'AVG_DAYS_TO_SELL':
                titles.append("Days to Sell by Month")
            elif col == 'ACTIVE_LISTINGS':
                titles.append("Active Listings by Month")
        
        fig = make_subplots(rows=subplot_rows, cols=subplot_cols, subplot_titles=titles)
        
        # Add traces for available columns
        plot_idx = 0
        for col in available_cols:
            row = plot_idx // subplot_cols + 1
            col_pos = plot_idx % subplot_cols + 1
            
            color = '#2196F3'  # Default color
            if col == 'LIKELY_SOLD':
                color = '#4CAF50'
            elif col == 'AVG_DAYS_TO_SELL':
                color = '#FF9800'
            elif col == 'ACTIVE_LISTINGS':
                color = '#9C27B0'
            
            fig.add_trace(
                go.Bar(
                    x=monthly_data['MONTH_NAME'],
                    y=monthly_data[col].apply(lambda x: float(x)),
                    marker_color=color,
                    name=col.replace('_', ' ').title()
                ),
                row=row, col=col_pos
            )
            
            plot_idx += 1
        
        # Update layout
        fig.update_layout(
            height=300 * subplot_rows,
            showlegend=False,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No metric data available for visualization.")
    
    # Show raw data
    with st.expander("View Raw Monthly Data"):
        st.dataframe(monthly_data)

# ======= MAIN APPLICATION =======
def main():
    """Main application function for the Market Analytics dashboard"""
    st.title("📈 RealtyLens Market Analytics")
    
    # Database indicator
    with st.sidebar:
        # Show a database indicator if the function is available
        try:
            render_db_indicator()
        except:
            pass
    
    # Listing type selection (tabs for Sales vs. Rentals)
    tab1, tab2 = st.tabs(["Sales Market", "Rental Market"])
    
    with tab1:
        st.header("Sales Market Analytics")
        
        # Initialize loading indicators
        market_placeholder = st.empty()
        with market_placeholder:
            st.info("Loading market health data...")
        
        price_placeholder = st.empty()
        with price_placeholder:
            st.info("Loading price analysis data...")
        
        elasticity_placeholder = st.empty()
        with elasticity_placeholder:
            st.info("Loading price elasticity data...")
        
        seasonality_placeholder = st.empty()
        with seasonality_placeholder:
            st.info("Loading seasonality data...")
        
        # Load data for sales market
        market_data = load_market_health_data(listing_type="sale")
        price_data = load_price_market_analysis(listing_type="sale")
        optimization_data = load_price_optimization_data(listing_type="sale")
        seasonality_data = load_seasonality_data()
        
        # Display visualizations
        with market_placeholder:
            st.markdown("### Sales Market Health Trends")
            plot_market_health_trends(market_data, listing_type="sale")
        
        with price_placeholder:
            display_price_analysis(price_data, listing_type="sale")
        
        with elasticity_placeholder:
            display_price_optimization(optimization_data, listing_type="sale")
        
        with seasonality_placeholder:
            display_sales_seasonality(seasonality_data)
    
    with tab2:
        st.header("Rental Market Analytics")
        
        # Initialize loading indicators
        market_placeholder = st.empty()
        with market_placeholder:
            st.info("Loading market health data...")
        
        price_placeholder = st.empty()
        with price_placeholder:
            st.info("Loading price analysis data...")
        
        lifecycle_placeholder = st.empty()
        with lifecycle_placeholder:
            st.info("Loading lifecycle data...")
        
        optimization_placeholder = st.empty()
        with optimization_placeholder:
            st.info("Loading price optimization data...")
        
        # Load data for rental market
        market_data = load_market_health_data(listing_type="rent")
        price_data = load_price_market_analysis(listing_type="rent")
        lifecycle_data = load_lifecycle_data()
        optimization_data = load_price_optimization_data(listing_type="rent")
        
        # Display visualizations
        with market_placeholder:
            st.markdown("### Rental Market Health Trends")
            plot_market_health_trends(market_data, listing_type="rent")
        
        with price_placeholder:
            display_price_analysis(price_data, listing_type="rent")
        
        with lifecycle_placeholder:
            display_rental_lifecycle(lifecycle_data)
        
        with optimization_placeholder:
            display_price_optimization(optimization_data, listing_type="rent")

# Run the application when the script is executed
if __name__ == "__main__":
    main()