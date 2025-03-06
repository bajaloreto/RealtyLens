import pandas as pd
import numpy as np
import pickle
import base64
import bz2
import gzip
import json
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def load_model_from_registry(hook, database, schema, model_registry_table, model_version=None):
    """
    Load a trained model from the model registry
    
    Parameters:
    hook: SnowflakeHook instance
    database: Database name
    schema: Schema name
    model_registry_table: Table containing the model registry
    model_version (str): The specific model version to load. If None, loads the latest model.
    
    Returns:
    model: The loaded ML model
    feature_info: Dictionary with feature information
    model_version: The model version used
    """
    print("Loading model from registry...")
    
    # First, check what columns actually exist in the table
    columns_query = f"""
    SELECT column_name 
    FROM {database}.information_schema.columns
    WHERE table_schema = '{schema}'
    AND table_name = '{model_registry_table}'
    """
    
    try:
        # Get available columns
        columns_df = hook.get_pandas_df(columns_query)
        available_columns = [col.upper() for col in columns_df['COLUMN_NAME'].tolist()]
        print(f"Available columns in {model_registry_table}: {available_columns}")
        
        # Determine which column to sort by based on what's available
        sort_column = None
        for col in ['CREATED_AT', 'MODEL_VERSION']:
            if col in available_columns:
                sort_column = col
                print(f"Will sort by {sort_column}")
                break
        
        if not sort_column:
            # No sortable column found, don't sort
            sort_column = 'MODEL_VERSION'  # Default, might cause error but better than nothing
        
        # Build query based on available columns
        if model_version:
            # Use specified model version
            model_query = f"""
            SELECT model_version, model_blob, feature_info, r2
            FROM {database}.{schema}.{model_registry_table}
            WHERE model_version = '{model_version}'
            AND model_blob IS NOT NULL
            """
        else:
            # Use the latest model - sort by chosen column
            model_query = f"""
            SELECT model_version, model_blob, feature_info, r2
            FROM {database}.{schema}.{model_registry_table}
            WHERE model_blob IS NOT NULL
            ORDER BY {sort_column} DESC
            LIMIT 1
            """
        
        model_data = hook.get_pandas_df(model_query)
        if len(model_data) == 0:
            raise ValueError("No model found in the registry")
            
        model_row = model_data.iloc[0]
        model_version = model_row['MODEL_VERSION']
        model_r2 = model_row['R2']
        
        print(f"Found model version {model_version} with R² = {model_r2:.4f}")
        
        # Decode and decompress model
        model_blob_b64 = model_row['MODEL_BLOB']
        model_blob = base64.b64decode(model_blob_b64)
        
        # Try different decompression methods
        try:
            # Try GZIP first (since we used it in training)
            model_pickle = gzip.decompress(model_blob)
            model = pickle.loads(model_pickle)
        except:
            # Fallback to BZ2
            try:
                model_pickle = bz2.decompress(model_blob)
                model = pickle.loads(model_pickle)
            except:
                # Last resort - try direct unpickling
                model = pickle.loads(model_blob)
        
        # Decode and decompress feature info
        feature_info_b64 = model_row['FEATURE_INFO']
        feature_info_blob = base64.b64decode(feature_info_b64)
        
        # Try GZIP decompression first
        try:
            feature_info_pickle = gzip.decompress(feature_info_blob)
            feature_info = pickle.loads(feature_info_pickle)
        except:
            # Fallback to other methods
            try:
                feature_info_pickle = bz2.decompress(feature_info_blob)
                feature_info = pickle.loads(feature_info_pickle)
            except:
                # Last resort
                feature_info = pickle.loads(feature_info_blob)
        
        print(f"Successfully loaded model with {len(feature_info.get('numerical', []))} numerical features " 
              f"and {len(feature_info.get('categorical', []))} categorical features")
        
        return model, feature_info, model_version
        
    except Exception as e:
        raise ValueError(f"Failed to load model: {e}")

def predict_rent_prices(snowflake_conn_id, database, schema, model_registry_table, model_version=None):
    """
    Use the trained model to predict rental prices for property sale listings
    
    Parameters:
    snowflake_conn_id: Snowflake connection ID
    database: Database name
    schema: Schema name
    model_registry_table: Table containing the model registry
    model_version (str): The specific model version to use. If None, uses the latest model.
    
    Returns:
    DataFrame: The sale listings with predicted rent prices
    """
    print("Starting rent price prediction...")
    
    # Create Snowflake hook
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    try:
        # Try to load the model
        model, feature_info, model_version = load_model_from_registry(
            hook, database, schema, model_registry_table, model_version)
    except Exception as e:
        print(f"WARNING: Could not load model from registry: {e}")
        print("Using simplified prediction method instead")
        
        # Use a simplified prediction method
        return simplified_prediction(hook, database, schema)
        
    # Get feature lists from feature_info
    categorical_features = feature_info.get('categorical', [])
    numerical_features = feature_info.get('numerical', [])
    
    # Print expected features for debugging
    print(f"Model expects these numerical features: {numerical_features}")
    print(f"Model expects these categorical features: {categorical_features}")
    
    # 1. Find the most recent load date
    max_load_date_query = f"""
    SELECT MAX(LOAD_DATE) AS MAX_LOAD_DATE
    FROM {database}.{schema}.FCT_SALE_LISTING
    """
    max_load_date_result = hook.get_pandas_df(max_load_date_query)
    max_load_date = max_load_date_result.iloc[0]['MAX_LOAD_DATE']
    print(f"Using data from most recent load date: {max_load_date}")
    
    # 3. Fetch sale listings and join with property dimension
    print("Fetching sale listing data...")
    listing_query = f"""
    SELECT 
        l.LISTING_SK,
        l.LISTING_ID,
        l.SALE_PRICE,
        l.DAYS_ON_MARKET,
        l.STATUS,
        l.LISTING_TYPE,
        l.LOAD_DATE,
        p.PROPERTY_TYPE,
        p.SQUARE_FOOTAGE,
        p.BEDROOMS,
        p.BATHROOMS,
        p.LOT_SIZE,
        p.YEAR_BUILT,
        p.ZONING_CODE,
        p.ZONING_GROUP,
        loc.CITY,
        loc.STATE,
        loc.ZIP_CODE,
        loc.COUNTY
    FROM 
        {database}.{schema}.FCT_SALE_LISTING l
    LEFT JOIN 
        {database}.{schema}.DIM_PROPERTY p ON l.PROPERTY_SK = p.PROPERTY_SK
    LEFT JOIN 
        {database}.{schema}.DIM_LOCATION loc ON l.LOCATION_SK = loc.LOCATION_SK
    WHERE 
        l.SALE_PRICE IS NOT NULL
        AND l.LOAD_DATE = '{max_load_date}'
    """
    
    listings_df = hook.get_pandas_df(listing_query)
    print(f"Fetched {len(listings_df)} sale listings from {max_load_date}")
    
    if len(listings_df) == 0:
        print(f"No listings found with sale prices for load date {max_load_date}.")
        return "No data to process"
    
    # 4. Prepare data for prediction
    print("Preparing data for prediction...")
    
    # Convert all columns to lowercase to match model's expected format
    listings_df.columns = [col.lower() for col in listings_df.columns]
    
    # Convert numerical columns to numeric types
    numeric_columns = ['sale_price', 'days_on_market', 'square_footage', 
                      'bedrooms', 'bathrooms', 'lot_size', 'year_built']
    
    for col in numeric_columns:
        if col in listings_df.columns:
            listings_df[col] = pd.to_numeric(listings_df[col], errors='coerce')
    
    # 5. Feature engineering - recreate the same derived features used in training
    print("Engineering features for prediction...")
    
    # Make a copy of the dataframe for prediction (with all needed features)
    X_pred = listings_df.copy()
    
    # Property age
    if 'property_age' in numerical_features:
        if 'year_built' in X_pred.columns:
            current_year = datetime.now().year
            X_pred['property_age'] = X_pred['year_built'].apply(
                lambda year: current_year - year if pd.notnull(year) and year > 1800 else np.nan
            )
        else:
            X_pred['property_age'] = np.nan
    
    # Bath to bed ratio
    if 'bath_to_bed_ratio' in numerical_features:
        if 'bathrooms' in X_pred.columns and 'bedrooms' in X_pred.columns:
            X_pred['bath_to_bed_ratio'] = X_pred.apply(
                lambda row: row['bathrooms'] / row['bedrooms']
                if pd.notnull(row['bedrooms']) and row['bedrooms'] > 0 
                   and pd.notnull(row['bathrooms'])
                else np.nan, axis=1
            )
        else:
            X_pred['bath_to_bed_ratio'] = np.nan
    
    # 6. Ensure all expected features exist in the prediction data
    # Check for missing numerical features and add them if needed
    for col in numerical_features:
        if col not in X_pred.columns:
            print(f"Adding missing numerical feature: {col}")
            X_pred[col] = np.nan
    
    # Check for missing categorical features and add them if needed
    for col in categorical_features:
        if col not in X_pred.columns:
            print(f"Adding missing categorical feature: {col}")
            X_pred[col] = None
    
    # 7. Select only the features expected by the model
    X_pred_model = X_pred[numerical_features + categorical_features]
    
    # 8. Make predictions
    print("Making predictions...")
    print(f"Prediction data shape: {X_pred_model.shape}")
    print(f"Features used for prediction: {X_pred_model.columns.tolist()}")
    
    try:
        # Make predictions
        rent_predictions = model.predict(X_pred_model)
        
        # Add predictions to the original DataFrame
        listings_df['predicted_rent_price'] = rent_predictions
        
        # Calculate investment metrics
        listings_df['rent_to_price_ratio'] = listings_df['predicted_rent_price'] / listings_df['sale_price']
        
        print("Prediction complete!")
        
        # 9. Create or replace the predictions table
        print("Creating predictions table...")
        hook.run(f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PREDICTED_RENT_PRICES (
            LISTING_SK VARCHAR,
            LISTING_ID VARCHAR,
            SALE_PRICE FLOAT,
            PREDICTED_RENT_PRICE FLOAT,
            RENT_TO_PRICE_RATIO FLOAT,
            LOAD_DATE DATE,
            MODEL_VERSION VARCHAR
        )
        """)
        
        # 10. Prepare prediction results for storage
        prediction_results = pd.DataFrame({
            'LISTING_SK': listings_df['listing_sk'],
            'LISTING_ID': listings_df['listing_id'],
            'SALE_PRICE': listings_df['sale_price'],
            'PREDICTED_RENT_PRICE': listings_df['predicted_rent_price'],
            'RENT_TO_PRICE_RATIO': listings_df['rent_to_price_ratio'],
            'LOAD_DATE': listings_df['load_date'],
            'MODEL_VERSION': model_version
        })
        
        # 11. Delete existing predictions for this load date
        hook.run(f"""
        DELETE FROM {database}.{schema}.PREDICTED_RENT_PRICES
        WHERE LOAD_DATE = '{max_load_date}'
        """)
        
        # 12. Insert new predictions
        print("Inserting predictions...")
        
        # Convert to records for insertion
        records = prediction_results.to_dict('records')
        
        # Batch insert
        rows_inserted = 0
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            values_list = []
            
            for row in batch:
                # Format each value appropriately for SQL
                listing_sk = f"'{row['LISTING_SK']}'" if pd.notna(row['LISTING_SK']) else "NULL"
                listing_id = f"'{row['LISTING_ID']}'" if pd.notna(row['LISTING_ID']) else "NULL"
                sale_price = f"{row['SALE_PRICE']}" if pd.notna(row['SALE_PRICE']) else "NULL"
                predicted_rent = f"{row['PREDICTED_RENT_PRICE']}" if pd.notna(row['PREDICTED_RENT_PRICE']) else "NULL"
                ratio = f"{row['RENT_TO_PRICE_RATIO']}" if pd.notna(row['RENT_TO_PRICE_RATIO']) else "NULL"
                load_date = f"'{row['LOAD_DATE']}'" if pd.notna(row['LOAD_DATE']) else "NULL"
                model_ver = f"'{model_version}'"
                
                values = f"({listing_sk}, {listing_id}, {sale_price}, {predicted_rent}, {ratio}, {load_date}, {model_ver})"
                values_list.append(values)
            
            if values_list:
                # Join all the value strings and execute the insert
                values_str = ",".join(values_list)
                insert_sql = f"""
                INSERT INTO {database}.{schema}.PREDICTED_RENT_PRICES
                (LISTING_SK, LISTING_ID, SALE_PRICE, PREDICTED_RENT_PRICE, RENT_TO_PRICE_RATIO, LOAD_DATE, MODEL_VERSION)
                VALUES {values_str}
                """
                hook.run(insert_sql)
                rows_inserted += len(batch)
        
        print(f"Saved {rows_inserted} predictions to table PREDICTED_RENT_PRICES")
        
        # 13. Generate summary statistics
        print("\nPrediction Summary Statistics:")
        print(f"Average Predicted Rent: ${listings_df['predicted_rent_price'].mean():.2f}")
        print(f"Median Predicted Rent: ${listings_df['predicted_rent_price'].median():.2f}")
        
        if 'property_type' in listings_df.columns:
            print("\nAverage Predicted Rent by Property Type:")
            property_summary = listings_df.groupby('property_type')['predicted_rent_price'].agg(['mean', 'count'])
            for property_type, stats in property_summary.iterrows():
                if not pd.isnull(property_type):
                    print(f"- {property_type}: ${stats['mean']:.2f} (count: {stats['count']})")
        
        return f"Successfully predicted rent prices for {len(listings_df)} listings"
        
    except Exception as e:
        print(f"Error making predictions: {e}")
        # More detailed error information
        print("\nDEBUG INFORMATION:")
        print(f"Prediction DataFrame Shape: {X_pred_model.shape}")
        print(f"Prediction DataFrame Columns: {X_pred_model.columns.tolist()}")
        print(f"Missing Values per Column:")
        print(X_pred_model.isnull().sum())
        
        # Check for NaN-only columns
        nan_cols = [col for col in X_pred_model.columns if X_pred_model[col].isna().all()]
        if nan_cols:
            print(f"Columns with all NaN values: {nan_cols}")
            
        raise 

def simplified_prediction(hook, database, schema):
    """Simplified prediction when no model is available"""
    # Find the most recent load date
    max_load_date_query = f"""
    SELECT MAX(LOAD_DATE) AS MAX_LOAD_DATE
    FROM {database}.{schema}.FCT_SALE_LISTING
    """
    max_load_date_result = hook.get_pandas_df(max_load_date_query)
    max_load_date = max_load_date_result.iloc[0]['MAX_LOAD_DATE']
    
    # Create the table if it doesn't exist
    hook.run(f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.PREDICTED_RENT_PRICES (
        LISTING_SK VARCHAR,
        LISTING_ID VARCHAR,
        SALE_PRICE FLOAT,
        PREDICTED_RENT_PRICE FLOAT,
        RENT_TO_PRICE_RATIO FLOAT,
        LOAD_DATE DATE,
        MODEL_VERSION VARCHAR
    )
    """)
    
    # Generate simple predictions (e.g., 0.5% of sale price)
    simple_pred_query = f"""
    INSERT INTO {database}.{schema}.PREDICTED_RENT_PRICES
    SELECT
        l.LISTING_SK,
        l.LISTING_ID,
        l.SALE_PRICE,
        l.SALE_PRICE * 0.005 AS PREDICTED_RENT_PRICE,
        0.005 AS RENT_TO_PRICE_RATIO,
        l.LOAD_DATE,
        'simplified-model' AS MODEL_VERSION
    FROM
        {database}.{schema}.FCT_SALE_LISTING l
    WHERE
        l.LOAD_DATE = '{max_load_date}'
        AND NOT EXISTS (
            SELECT 1 FROM {database}.{schema}.PREDICTED_RENT_PRICES p
            WHERE p.LISTING_SK = l.LISTING_SK
        )
    """
    
    hook.run(simple_pred_query)
    
    return "Completed simplified rent price prediction" 