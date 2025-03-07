# Market Analytics Page for RealtyLens
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import math

# Import core functions from main app for Snowflake connection
from Property_Map import query_snowflake, render_db_indicator

# Page title and content (without set_page_config)
st.title("📊 RealtyLens Market Analytics")

def load_table_data(table_name):
    """Load data from a specified table in Snowflake"""
    try:
        query = f"SELECT * FROM DATAEXPERT_STUDENT.JMUSNI07.{table_name}"
        data = query_snowflake(query)
        return data
    except Exception as e:
        st.error(f"Error loading {table_name}: {str(e)}")
        return None

def visualize_rent_lifecycle(data):
    """Visualize rental property lifecycle stages"""
    if data is None or data.empty:
        st.warning("No data available for RENT_LIFECYCLE")
        return
    
    # Sort by property count for better visualization
    data = data.sort_values(by="PROPERTY_COUNT", ascending=False)
    
    # Create funnel chart
    fig = px.funnel(
        data,
        x="PROPERTY_COUNT",
        y="LIFECYCLE_STAGE",
        title="Rental Property Lifecycle Funnel"
    )
    
    # Add conversion rate as text
    fig.update_traces(
        texttemplate="<b>%{y}</b><br>%{x} properties<br>Conv. Rate: %{customdata[0]:.1%}",
        customdata=data[["CONVERSION_RATE"]]
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show additional metrics
    col1, col2 = st.columns(2)
    with col1:
        avg_days = data["AVG_DAYS_TO_CONVERSION"].mean()
        st.metric("Average Days to Conversion", f"{avg_days:.1f} days")
    
    with col2:
        price_drop_rate = data["PRICE_DROP_RATE"].mean()
        st.metric("Average Price Drop Rate", f"{price_drop_rate:.1%}")

def visualize_rent_market_health(data):
    """Visualize rental market health over time"""
    if data is None or data.empty:
        st.warning("No data available for RENT_MARKET_HEALTH_INDEX")
        return
    
    # Sort data by date in ascending order
    data = data.sort_values(by="DAY", ascending=True)
    
    # Convert DAY column to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(data["DAY"]):
        data["DAY"] = pd.to_datetime(data["DAY"])
    
    # Convert decimal columns to float to avoid type errors
    numeric_columns = ["MARKET_HEALTH_SCORE", "AVG_DAYS_ON_MARKET", "TOTAL_LISTINGS", 
                       "NEW_LISTING_RATE", "PRICE_CHANGE_PCT", "PRICE_INCREASE_RATE", 
                       "PRICE_DECREASE_RATE", "SUPPLY_DEMAND_RATIO"]
    
    for col in numeric_columns:
        if col in data.columns:
            data[col] = data[col].astype(float)
    
    # Calculate safe max values to avoid errors
    max_health = float(data["MARKET_HEALTH_SCORE"].max()) * 1.1
    max_days = float(data["AVG_DAYS_ON_MARKET"].max()) * 1.1
    max_listings = float(data["TOTAL_LISTINGS"].max()) * 1.1
    
    # Create time series for market health
    fig = go.Figure()
    
    # Add health score
    fig.add_trace(go.Scatter(
        x=data["DAY"],
        y=data["MARKET_HEALTH_SCORE"],
        name="Market Health Score",
        line=dict(color="green", width=3),
        hovertemplate="Date: %{x|%Y-%m-%d}<br>Health Score: %{y:.2f}<extra></extra>"
    ))
    
    # Add days on market
    fig.add_trace(go.Scatter(
        x=data["DAY"],
        y=data["AVG_DAYS_ON_MARKET"],
        name="Avg Days on Market",
        line=dict(color="orange", width=2),
        yaxis="y2",
        hovertemplate="Date: %{x|%Y-%m-%d}<br>Avg Days: %{y:.1f} days<extra></extra>"
    ))
    
    # Add total listings as a third trace
    fig.add_trace(go.Scatter(
        x=data["DAY"],
        y=data["TOTAL_LISTINGS"],
        name="Total Listings",
        line=dict(color="blue", width=1, dash="dot"),
        yaxis="y3",
        hovertemplate="Date: %{x|%Y-%m-%d}<br>Listings: %{y:,}<extra></extra>"
    ))
    
    # Update layout with better formatting
    fig.update_layout(
        title="Rental Market Health Trends Over Time",
        xaxis=dict(
            title="Date",
            tickformat="%b %d, %Y"
        ),
        yaxis=dict(
            title="Market Health Score",
            side="left",
            range=[0, max_health]
        ),
        yaxis2=dict(
            title="Avg Days on Market",
            overlaying="y",
            side="right",
            range=[0, max_days]
        ),
        yaxis3=dict(
            title="Total Listings",
            overlaying="y",
            side="right",
            position=0.9,
            anchor="free",
            range=[0, max_listings]
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=50, r=100, t=80, b=50),
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show latest metrics
    if len(data) > 0:
        latest = data.iloc[-1]  # Get the most recent data point
        prev = data.iloc[-2] if len(data) > 1 else None  # Get previous data point if available
        
        st.subheader("Latest Market Indicators")
        
        # Create three columns for metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            delta = None
            if prev is not None:
                delta = float(latest['MARKET_HEALTH_SCORE']) - float(prev['MARKET_HEALTH_SCORE'])
            
            st.metric("Market Health Score", 
                     f"{float(latest['MARKET_HEALTH_SCORE']):.2f}",
                     f"{delta:.2f}" if delta is not None else None)
        
        with col2:
            delta = None
            if prev is not None:
                delta = -(float(latest['AVG_DAYS_ON_MARKET']) - float(prev['AVG_DAYS_ON_MARKET']))  # Negative change is good
            
            st.metric("Avg Days on Market", 
                     f"{float(latest['AVG_DAYS_ON_MARKET']):.1f} days",
                     f"{delta:.1f} days" if delta is not None else None)
        
        with col3:
            delta_percent = None
            if prev is not None and float(prev['TOTAL_LISTINGS']) > 0:
                delta_percent = (float(latest['TOTAL_LISTINGS']) - float(prev['TOTAL_LISTINGS'])) / float(prev['TOTAL_LISTINGS']) * 100
            
            st.metric("Total Listings", 
                     f"{int(latest['TOTAL_LISTINGS']):,}",
                     f"{delta_percent:.1f}%" if delta_percent is not None else None)
        
        # Second row of metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("New Listing Rate", 
                     f"{float(latest['NEW_LISTING_RATE']):.1%}")
        
        with col2:
            st.metric("Price Change", 
                     f"{float(latest['PRICE_CHANGE_PCT']):.1%}",
                     f"{float(latest['PRICE_INCREASE_RATE']) - float(latest['PRICE_DECREASE_RATE']):.1%}")
        
        with col3:
            st.metric("Supply/Demand Ratio", 
                     f"{float(latest['SUPPLY_DEMAND_RATIO']):.2f}",
                     "Higher = more supply")

def visualize_price_market_analysis(data, market_type="rental"):
    """Visualize price market analysis for either rental or sales market"""
    if data is None or data.empty:
        st.warning(f"No data available for {market_type.upper()}_PRICE_MARKET_ANALYSIS")
        return
    
    # Set appropriate column names based on market type
    price_col = "AVG_RENT_PRICE" if market_type == "rental" else "AVG_SALE_PRICE"
    min_price_col = "MIN_RENT_PRICE" if market_type == "rental" else "MIN_SALE_PRICE"
    max_price_col = "MAX_RENT_PRICE" if market_type == "rental" else "MAX_SALE_PRICE"
    
    # Filter controls
    agg_levels = sorted(data["AGGREGATION_LEVEL"].unique())
    selected_agg = st.selectbox(
        "Select Aggregation Level",
        agg_levels,
        key=f"{market_type}_price_agg"
    )
    
    # Filter data
    filtered_data = data[data["AGGREGATION_LEVEL"] == selected_agg].copy()
    
    if filtered_data.empty:
        st.warning(f"No data available for {selected_agg} aggregation level")
        return
    
    # Handle different aggregation levels
    if selected_agg == "property_type":
        x_col = "PROPERTY_TYPE"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Property Type"
    
    elif selected_agg == "status":
        x_col = "STATUS"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Status"
    
    elif selected_agg == "bedrooms":
        x_col = "BEDROOMS"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Bedroom Count"
    
    elif selected_agg == "property_type__status":
        # Create combined label for property type and status
        filtered_data["LABEL"] = filtered_data["PROPERTY_TYPE"] + " - " + filtered_data["STATUS"]
        x_col = "LABEL"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Property Type and Status"
    
    elif selected_agg == "property_type__bedrooms":
        # Create combined label for property type and bedrooms
        filtered_data["LABEL"] = filtered_data["PROPERTY_TYPE"] + " - " + filtered_data["BEDROOMS"] + " BR"
        x_col = "LABEL"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Property Type and Bedrooms"
    
    elif selected_agg == "status__bedrooms":
        # Create combined label for status and bedrooms
        filtered_data["LABEL"] = filtered_data["STATUS"] + " - " + filtered_data["BEDROOMS"] + " BR"
        x_col = "LABEL"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Status and Bedrooms"
    
    elif selected_agg == "property_type__status__bedrooms":
        # Create combined label for all three dimensions
        filtered_data["LABEL"] = filtered_data["PROPERTY_TYPE"] + " - " + filtered_data["STATUS"] + " - " + filtered_data["BEDROOMS"] + " BR"
        x_col = "LABEL"
        title = f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price by Property Type, Status, and Bedrooms"
    
    elif selected_agg == "overall":
        # For overall, create a simpler visualization
        overall_price = filtered_data[price_col].values[0]
        overall_min = filtered_data[min_price_col].values[0]
        overall_max = filtered_data[max_price_col].values[0]
        listing_count = filtered_data["LISTING_COUNT"].values[0]
        
        st.subheader(f"Overall {market_type.capitalize()} Market Summary")
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Listings", f"{listing_count:,}")
        with col2:
            st.metric(f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price", f"${overall_price:,.2f}")
        with col3:
            st.metric("Price Range", f"${overall_min:,.2f} - ${overall_max:,.2f}")
        
        # Create a gauge chart
        fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = overall_price,
            number = {"prefix": "$", "valueformat": ",.0f"},
            title = {"text": f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price"},
            gauge = {
                "axis": {"range": [overall_min, overall_max]},
                "bar": {"color": "darkblue"},
                "steps": [
                    {"range": [overall_min, overall_min + (overall_max-overall_min)/3], "color": "lightgray"},
                    {"range": [overall_min + (overall_max-overall_min)/3, overall_min + 2*(overall_max-overall_min)/3], "color": "gray"}
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": overall_price
                }
            }
        ))
        
        st.plotly_chart(fig, use_container_width=True)
        return
    
    else:
        st.error(f"Unsupported aggregation level: {selected_agg}")
        st.write("Available columns:", filtered_data.columns.tolist())
        st.dataframe(filtered_data)
        return
    
    # Sort data for better visualization (by listing count)
    filtered_data = filtered_data.sort_values("LISTING_COUNT", ascending=False)
    
    # Truncate labels if there are too many
    if len(filtered_data) > 15:
        st.warning(f"Showing top 15 segments out of {len(filtered_data)} total")
        filtered_data = filtered_data.head(15)
    
    # Create visualization
    fig = px.bar(
        filtered_data,
        x=x_col,
        y=price_col,
        text="LISTING_COUNT",
        title=title,
        labels={
            x_col: "Market Segment", 
            price_col: f"Average {'Rent' if market_type == 'rental' else 'Sale'} Price ($)",
            "LISTING_COUNT": "Number of Listings"
        }
    )
    
    # Add error bars
    fig.update_traces(
        error_y=dict(
            type="data",
            symmetric=False,
            array=filtered_data[max_price_col] - filtered_data[price_col],
            arrayminus=filtered_data[price_col] - filtered_data[min_price_col]
        ),
        texttemplate="%{text} listings",
        textposition="outside"
    )
    
    # Rotate x-axis labels if they are combined
    if selected_agg != "property_type" and selected_agg != "status" and selected_agg != "bedrooms":
        fig.update_layout(
            xaxis=dict(
                tickangle=45,
                title_standoff=25
            ),
            margin=dict(b=150)
        )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show data table
    with st.expander("View Data Details"):
        st.dataframe(filtered_data)

def visualize_rent_price_optimization(data):
    """Visualize rental price optimization strategies"""
    if data is None or data.empty:
        st.warning("No data available for RENT_PRICE_OPTIMIZATION")
        return
    
    # Create scatter plot
    fig = px.scatter(
        data,
        x="AVG_DAYS_ON_MARKET",
        y="CONVERSION_RATE",
        size="PROPERTY_COUNT",
        color="PRICE_STRATEGY",
        hover_name="PRICE_QUINTILE",
        text="PRICE_QUINTILE",
        title="Rental Price Optimization Strategies",
        labels={
            "AVG_DAYS_ON_MARKET": "Average Days on Market",
            "CONVERSION_RATE": "Conversion Rate",
            "PROPERTY_COUNT": "Property Count",
            "PRICE_STRATEGY": "Price Strategy"
        }
    )
    
    # Update traces
    fig.update_traces(
        textposition="top center",
        marker=dict(sizemode="area", sizeref=0.1)
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Average Days on Market",
        yaxis_title="Conversion Rate",
        yaxis_tickformat=".0%"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show optimization insights
    st.subheader("Price Optimization Insights")
    
    # Group by price strategy for insights
    strategy_data = data.groupby("PRICE_STRATEGY").agg({
        "PROPERTY_COUNT": "sum",
        "AVG_DAYS_ON_MARKET": "mean",
        "CONVERSION_RATE": "mean",
        "AVG_PRICE_ADJUSTMENT_PCT": "mean"
    }).reset_index()
    
    col1, col2, col3 = st.columns(3)
    
    for i, row in strategy_data.iterrows():
        with col1 if i == 0 else col2 if i == 1 else col3:
            st.metric(
                f"{row['PRICE_STRATEGY']} Strategy",
                f"{row['CONVERSION_RATE']:.1%} Conv. Rate",
                f"{row['AVG_DAYS_ON_MARKET']:.1f} DOM"
            )

def visualize_sale_market_timing(data):
    """Visualize sales market timing and seasonality"""
    if data is None or data.empty:
        st.warning("No data available for SALE_MARKET_TIMING_AND_SEASONALITY")
        return
    
    # Create time series visualization
    fig = go.Figure()
    
    # Add market velocity
    fig.add_trace(go.Scatter(
        x=data["YEAR_MONTH"],
        y=data["MARKET_VELOCITY"],
        name="Market Velocity",
        line=dict(color="blue", width=3)
    ))
    
    # Add months of inventory
    fig.add_trace(go.Scatter(
        x=data["YEAR_MONTH"],
        y=data["MONTHS_OF_INVENTORY"],
        name="Months of Inventory",
        line=dict(color="red", width=2),
        yaxis="y2"
    ))
    
    # Update layout
    fig.update_layout(
        title="Sales Market Timing and Inventory",
        xaxis_title="Month",
        yaxis_title="Market Velocity",
        yaxis2=dict(
            title="Months of Inventory",
            overlaying="y",
            side="right"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show seasonality by price segment
    if "PRICE_SEGMENT" in data.columns and "SEASONALITY_INDEX" in data.columns:
        # Group by price segment
        segment_data = data.groupby("PRICE_SEGMENT").agg({
            "SEASONALITY_INDEX": "mean",
            "DISCOUNT_PRESSURE": "mean",
            "AVG_DAYS_TO_SELL": "mean"
        }).reset_index()
        
        # Create bar chart
        fig = px.bar(
            segment_data,
            x="PRICE_SEGMENT",
            y="SEASONALITY_INDEX",
            color="DISCOUNT_PRESSURE",
            title="Seasonality by Price Segment",
            labels={
                "PRICE_SEGMENT": "Price Segment",
                "SEASONALITY_INDEX": "Seasonality Index",
                "DISCOUNT_PRESSURE": "Discount Pressure"
            }
        )
        
        st.plotly_chart(fig, use_container_width=True)

def visualize_sale_price_elasticity(data):
    """Visualize sales price elasticity and discount impact"""
    if data is None or data.empty:
        st.warning("No data available for SALE_PRICE_ELASTICITY_AND_DISCOUNT_IMPACT")
        return
    
    # Create heatmap-like visualization
    fig = px.scatter(
        data,
        x="PRICE_SEGMENT",
        y="DAYS_SEGMENT",
        size="LISTING_COUNT",
        color="CONVERSION_RATE",
        hover_data=["AVG_PRICE_ADJUSTMENT_PCT", "MARKET_EFFICIENCY_SCORE"],
        title="Sales Price Elasticity by Time on Market",
        color_continuous_scale="RdYlGn",
        labels={
            "PRICE_SEGMENT": "Price Segment",
            "DAYS_SEGMENT": "Days on Market Segment",
            "CONVERSION_RATE": "Conversion Rate",
            "LISTING_COUNT": "Listing Count"
        }
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Price Segment",
        yaxis_title="Days on Market Segment",
        coloraxis_colorbar=dict(title="Conversion Rate")
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show efficiency analysis
    st.subheader("Market Efficiency Analysis")
    
    # Group by price segment
    price_data = data.groupby("PRICE_SEGMENT").agg({
        "LISTING_COUNT": "sum",
        "CONVERSION_RATE": "mean",
        "AVG_DAYS_TO_SELL": "mean",
        "MARKET_EFFICIENCY_SCORE": "mean"
    }).reset_index()
    
    # Create score chart
    fig = px.bar(
        price_data,
        x="PRICE_SEGMENT",
        y="MARKET_EFFICIENCY_SCORE",
        text="MARKET_EFFICIENCY_SCORE",
        color="AVG_DAYS_TO_SELL",
        title="Market Efficiency by Price Segment",
        labels={
            "PRICE_SEGMENT": "Price Segment",
            "MARKET_EFFICIENCY_SCORE": "Market Efficiency Score",
            "AVG_DAYS_TO_SELL": "Avg. Days to Sell"
        }
    )
    
    fig.update_traces(
        texttemplate="%{text:.2f}",
        textposition="outside"
    )
    
    st.plotly_chart(fig, use_container_width=True)

def visualize_rental_lifecycle(data):
    """Visualize rental property lifecycle and retention over time"""
    if data is None or data.empty:
        st.warning("No data available for RENT_MARKET_HEALTH_INDEX")
        return
    
    # Sort data by date in ascending order
    data = data.sort_values(by="DAY", ascending=True)
    
    # Convert DAY column to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(data["DAY"]):
        data["DAY"] = pd.to_datetime(data["DAY"])
    
    # Convert decimal columns to float to avoid type errors
    numeric_columns = [
        "TOTAL_LISTINGS", "NEW_LISTINGS", "RETAINED_LISTINGS", 
        "CHURNED_LISTINGS", "RESURRECTED_LISTINGS", "INACTIVE_LISTINGS",
        "NEW_LISTING_RATE", "CHURN_RATE", "RESURRECTION_RATE"
    ]
    
    for col in numeric_columns:
        if col in data.columns:
            data[col] = data[col].astype(float)
    
    # Create tabs for different lifecycle visualizations
    tab1, tab2, tab3 = st.tabs(["Listing Flow", "Retention Analysis", "Listing Status Breakdown"])
    
    # Tab 1: Listing Flow - Waterfall/Flow visualization
    with tab1:
        st.subheader("Rental Listing Flow Over Time")
        
        # Use the most recent date range (last 6 months if available)
        if len(data) > 180:
            recent_data = data.tail(180)
        else:
            recent_data = data
        
        # Create listing flow chart (area chart)
        fig = go.Figure()
        
        # Add traces for different listing statuses
        fig.add_trace(go.Scatter(
            x=recent_data["DAY"], 
            y=recent_data["NEW_LISTINGS"],
            mode='lines',
            stackgroup='one',
            name='New Listings',
            line=dict(width=0.5, color='rgb(0, 180, 0)'),
            hovertemplate='%{y:,.0f} new listings<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=recent_data["DAY"], 
            y=recent_data["RETAINED_LISTINGS"],
            mode='lines',
            stackgroup='one',
            name='Retained Listings',
            line=dict(width=0.5, color='rgb(0, 100, 180)'),
            hovertemplate='%{y:,.0f} retained listings<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=recent_data["DAY"], 
            y=recent_data["RESURRECTED_LISTINGS"],
            mode='lines',
            stackgroup='one',
            name='Resurrected Listings',
            line=dict(width=0.5, color='rgb(180, 180, 0)'),
            hovertemplate='%{y:,.0f} resurrected listings<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title="Rental Listing Flow",
            xaxis_title="Date",
            yaxis_title="Number of Listings",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate and display listing flow metrics
        st.subheader("Listing Flow Metrics")
        
        # Get latest data point
        latest = recent_data.iloc[-1]
        
        # Calculate what percentage each type contributes to total active listings
        active_total = float(latest["NEW_LISTINGS"] + latest["RETAINED_LISTINGS"] + latest["RESURRECTED_LISTINGS"])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            new_pct = (float(latest["NEW_LISTINGS"]) / active_total * 100) if active_total > 0 else 0
            st.metric("New Listings", f"{int(latest['NEW_LISTINGS']):,}", f"{new_pct:.1f}% of active")
        
        with col2:
            retained_pct = (float(latest["RETAINED_LISTINGS"]) / active_total * 100) if active_total > 0 else 0
            st.metric("Retained Listings", f"{int(latest['RETAINED_LISTINGS']):,}", f"{retained_pct:.1f}% of active")
        
        with col3:
            resurrected_pct = (float(latest["RESURRECTED_LISTINGS"]) / active_total * 100) if active_total > 0 else 0
            st.metric("Resurrected Listings", f"{int(latest['RESURRECTED_LISTINGS']):,}", f"{resurrected_pct:.1f}% of active")
    
    # Tab 2: Retention Analysis
    with tab2:
        st.subheader("Rental Market Retention Analysis")
        
        # Calculate retention rate
        data["RETENTION_RATE"] = data["RETAINED_LISTINGS"] / (data["RETAINED_LISTINGS"] + data["CHURNED_LISTINGS"]) * 100
        data["RETENTION_RATE"] = data["RETENTION_RATE"].fillna(0)
        
        # Create retention visualization
        fig = go.Figure()
        
        # Add retention rate
        fig.add_trace(go.Scatter(
            x=data["DAY"],
            y=data["RETENTION_RATE"],
            mode="lines",
            name="Retention Rate (%)",
            line=dict(color="green", width=3),
            hovertemplate="Date: %{x|%Y-%m-%d}<br>Retention Rate: %{y:.1f}%<extra></extra>"
        ))
        
        # Add churn rate on secondary y-axis
        fig.add_trace(go.Scatter(
            x=data["DAY"],
            y=data["CHURN_RATE"] * 100,  # Convert to percentage
            mode="lines",
            name="Churn Rate (%)",
            line=dict(color="red", width=2, dash="dot"),
            yaxis="y2",
            hovertemplate="Date: %{x|%Y-%m-%d}<br>Churn Rate: %{y:.1f}%<extra></extra>"
        ))
        
        # Update layout
        fig.update_layout(
            title="Rental Listing Retention vs. Churn Rate",
            xaxis_title="Date",
            yaxis_title="Retention Rate (%)",
            yaxis2=dict(
                title="Churn Rate (%)",
                overlaying="y",
                side="right",
                range=[0, max(data["CHURN_RATE"] * 100) * 1.1]
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode="x unified"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Retention metrics
        latest = data.iloc[-1]
        avg_retention = data["RETENTION_RATE"].mean()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "Current Retention Rate", 
                f"{latest['RETENTION_RATE']:.1f}%",
                f"{latest['RETENTION_RATE'] - avg_retention:.1f}% vs avg"
            )
            
        with col2:
            st.metric(
                "Current Churn Rate", 
                f"{latest['CHURN_RATE'] * 100:.1f}%",
                f"{(latest['CHURN_RATE'] - data['CHURN_RATE'].mean()) * 100:.1f}% vs avg"
            )
    
    # Tab 3: Listing Status Breakdown
    with tab3:
        st.subheader("Rental Listing Status Breakdown")
        
        # Create pie chart for latest listing status breakdown
        latest = data.iloc[-1]
        
        # Create dataframe for the pie chart
        status_data = pd.DataFrame({
            "Status": ["New", "Retained", "Churned", "Resurrected", "Inactive"],
            "Count": [
                float(latest["NEW_LISTINGS"]),
                float(latest["RETAINED_LISTINGS"]),
                float(latest["CHURNED_LISTINGS"]),
                float(latest["RESURRECTED_LISTINGS"]),
                float(latest["INACTIVE_LISTINGS"])
            ]
        })
        
        # Create pie chart
        fig = px.pie(
            status_data, 
            names="Status", 
            values="Count",
            title=f"Listing Status Breakdown ({latest['DAY'].strftime('%Y-%m-%d')})",
            color="Status",
            color_discrete_map={
                "New": "#00B050",
                "Retained": "#0064B4", 
                "Churned": "#FF0000",
                "Resurrected": "#B4B400",
                "Inactive": "#808080"
            }
        )
        
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            hovertemplate="%{label}<br>Count: %{value:,.0f}<br>Percentage: %{percent:.1%}<extra></extra>"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show trends over time for each status
        status_breakdown = data[["DAY", "NEW_LISTINGS", "RETAINED_LISTINGS", 
                               "CHURNED_LISTINGS", "RESURRECTED_LISTINGS", 
                               "INACTIVE_LISTINGS"]].copy()
        
        # Plot line chart showing trends
        status_names = {
            "NEW_LISTINGS": "New",
            "RETAINED_LISTINGS": "Retained",
            "CHURNED_LISTINGS": "Churned",
            "RESURRECTED_LISTINGS": "Resurrected",
            "INACTIVE_LISTINGS": "Inactive"
        }
        
        fig = go.Figure()
        
        for col, name in status_names.items():
            fig.add_trace(go.Scatter(
                x=status_breakdown["DAY"],
                y=status_breakdown[col],
                mode="lines",
                name=name
            ))
        
        fig.update_layout(
            title="Listing Status Trends Over Time",
            xaxis_title="Date",
            yaxis_title="Number of Listings",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode="x unified"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add metrics for current period vs previous period
        if len(data) > 30:  # If we have at least a month of data
            current = data.iloc[-1]
            previous = data.iloc[-31]  # Data from ~30 days ago
            
            st.subheader("Month-over-Month Changes")
            
            cols = st.columns(5)
            metrics = [
                ("New Listings", "NEW_LISTINGS"), 
                ("Retained Listings", "RETAINED_LISTINGS"),
                ("Churned Listings", "CHURNED_LISTINGS"),
                ("Resurrected", "RESURRECTED_LISTINGS"),
                ("Inactive", "INACTIVE_LISTINGS")
            ]
            
            for i, (label, col_name) in enumerate(metrics):
                with cols[i]:
                    current_val = float(current[col_name])
                    prev_val = float(previous[col_name])
                    change_pct = ((current_val - prev_val) / prev_val * 100) if prev_val > 0 else float('inf')
                    
                    # Format the delta to show percentage change
                    delta = f"{change_pct:+.1f}%" if not pd.isna(change_pct) and not math.isinf(change_pct) else None
                    
                    st.metric(label, f"{int(current_val):,}", delta)

def main():
    st.title("Market Analytics")
    
    # Database indicator in sidebar
    with st.sidebar:
        render_db_indicator()
        
        st.subheader("Data Tables")
        table_option = st.radio(
            "Select Data Table",
            [
                "Sales Price Analysis",
                "Rental Price Analysis",
                "Rental Lifecycle", 
                "Rental Market Health",
                "Rental Price Optimization",
                "Sales Market Timing",
                "Sales Price Elasticity"
            ]
        )
    
    # Load and display data based on selection
    if table_option == "Sales Price Analysis":
        data = load_table_data("SALE_PRICE_MARKET_ANALYSIS")
        st.header("Sales Price Market Analysis")
        visualize_price_market_analysis(data, market_type="sale")
        
    elif table_option == "Rental Price Analysis":
        data = load_table_data("RENT_PRICE_MARKET_ANALYSIS")
        st.header("Rental Price Market Analysis")
        visualize_price_market_analysis(data, market_type="rental")
        
    elif table_option == "Rental Lifecycle":
        data = load_table_data("RENT_LIFECYCLE")
        st.header("Rental Lifecycle Analysis")
        visualize_rent_lifecycle(data)
        
    elif table_option == "Rental Market Health":
        data = load_table_data("RENT_MARKET_HEALTH_INDEX")
        st.header("Rental Property Lifecycle Analysis")
        visualize_rental_lifecycle(data)
        
    elif table_option == "Rental Price Optimization":
        data = load_table_data("RENT_PRICE_OPTIMIZATION")
        st.header("Rental Price Optimization")
        visualize_rent_price_optimization(data)
        
    elif table_option == "Sales Market Timing":
        data = load_table_data("SALE_MARKET_TIMING_AND_SEASONALITY")
        st.header("Sales Market Timing and Seasonality")
        visualize_sale_market_timing(data)
        
    elif table_option == "Sales Price Elasticity":
        data = load_table_data("SALE_PRICE_ELASTICITY_AND_DISCOUNT_IMPACT")
        st.header("Sales Price Elasticity and Discount Impact")
        visualize_sale_price_elasticity(data)
    
    # Show raw data if requested
    if st.checkbox("Show Raw Data"):
        if table_option == "Rental Lifecycle":
            data = load_table_data("RENT_LIFECYCLE")
        elif table_option == "Rental Market Health":
            data = load_table_data("RENT_MARKET_HEALTH_INDEX")
        elif table_option == "Rental Price Analysis":
            data = load_table_data("RENT_PRICE_MARKET_ANALYSIS")
        elif table_option == "Rental Price Optimization":
            data = load_table_data("RENT_PRICE_OPTIMIZATION")
        elif table_option == "Sales Market Timing":
            data = load_table_data("SALE_MARKET_TIMING_AND_SEASONALITY")
        elif table_option == "Sales Price Elasticity":
            data = load_table_data("SALE_PRICE_ELASTICITY_AND_DISCOUNT_IMPACT")
        elif table_option == "Sales Price Analysis":
            data = load_table_data("SALE_PRICE_MARKET_ANALYSIS")
            
        st.dataframe(data)

if __name__ == "__main__":
    main()