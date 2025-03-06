"""
### Rent Price Prediction Model Training Workflow with Snowpark

This DAG:
1. Creates a Snowpark ML model registry if it doesn't exist
2. Accesses the map-based feature store in Snowflake 
3. Trains a Gradient Boosting Regressor for rent price prediction
4. Registers the model in Snowflake's model registry
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonVirtualenvOperator, ExternalPythonOperator

# Configuration
SNOWFLAKE_CONN_ID = "snowflake_conn"  # Using same connection ID as your daily DAG
MY_DATABASE = "dataexpert_student"
MY_SCHEMA = "jmusni07"
FEATURE_STORE_TABLE = "feature_store_rent_price_map"
MODEL_REGISTRY_TABLE = "model_registry"

# Define the function outside the DAG - this is a crucial fix
def train_rent_price_model(snowflake_conn_id, database, schema, feature_store_table, model_registry_table):
    """Train the rent price prediction model using scikit-learn"""
    # Use SnowflakeHook to fetch data
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Fetch data
    df_map = hook.get_pandas_df(f"""
        SELECT * FROM {database}.{schema}.{feature_store_table}
    """)
    
    print(f"Fetched {len(df_map)} rows from feature store")
    print(f"DataFrame columns: {df_map.columns.tolist()}")
    
    # The ML processing code
    import pandas as pd
    import numpy as np
    import json
    import pickle
    import base64
    import gzip
    from datetime import datetime
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.metrics import mean_squared_error, r2_score
    
    # Flatten the map-based data structure
    print("Flattening map structure...")
    df = pd.DataFrame()
    
    # Standardize column names for case-insensitivity
    columns = {c.lower(): c for c in df_map.columns}
    
    # Get the correct column names from the DataFrame
    record_id_col = columns.get('record_id')
    categorical_col = columns.get('categorical_features')
    numerical_col = columns.get('numerical_features')
    target_col = columns.get('target_variable')
    metadata_col = columns.get('metadata')
    
    # Parse RECORD_ID JSON
    df['listing_id'] = df_map[record_id_col].apply(lambda x: json.loads(x).get('listing_id') if x else None)
    df['property_id'] = df_map[record_id_col].apply(lambda x: json.loads(x).get('property_id') if x else None)
    
    # Extract categorical features
    categorical_fields = ['property_type', 'city', 'state', 'zip_code', 
                         'county', 'status', 'listing_type', 'zoning_code', 'zoning_group']
    
    for field in categorical_fields:
        df[field] = df_map[categorical_col].apply(
            lambda x: json.loads(x).get(field) if x else None
        )
    
    # Extract numerical features
    numerical_fields = ['bedrooms', 'bathrooms', 'square_footage', 
                       'lot_size', 'year_built', 'days_on_market']
    
    for field in numerical_fields:
        df[field] = df_map[numerical_col].apply(
            lambda x: json.loads(x).get(field) if x else None
        )
    
    # Extract target variable (rent_price)
    df['rent_price'] = df_map[target_col].apply(
        lambda x: json.loads(x).get('rent_price') if x else None
    )
    
    # Extract metadata (is_active and data_partition)
    df['is_active'] = df_map[metadata_col].apply(
        lambda x: json.loads(x).get('is_active') if x else True
    )
    
    df['data_partition'] = df_map[metadata_col].apply(
        lambda x: json.loads(x).get('data_partition') if x else 'TRAIN'
    )
    
    # Filter active records
    df = df[df['is_active'] == True]
    
    # Data cleaning and preparation
    print("Performing data cleaning...")
    
    # Drop identifier columns
    df_modeling = df.drop(['listing_id', 'property_id', 'is_active', 'data_partition'], axis=1)
    
    # Define feature groups
    categorical = [col for col in categorical_fields if col in df_modeling.columns]
    numerical = [col for col in numerical_fields if col in df_modeling.columns]
    
    # Convert numerical columns to numeric type
    for col in numerical:
        if col in df_modeling.columns:
            df_modeling[col] = pd.to_numeric(df_modeling[col], errors='coerce')
    
    # Handle outliers in numerical columns
    for col in numerical:
        if col in df_modeling.columns and df_modeling[col].notna().any():
            upper_limit = df_modeling[col].quantile(0.99)
            df_modeling[col] = df_modeling[col].clip(upper=upper_limit)
    
    # Feature engineering
    print("Performing feature engineering...")
    
    # Property age
    if 'year_built' in df_modeling.columns and df_modeling['year_built'].notna().any():
        current_year = datetime.now().year
        df_modeling['property_age'] = df_modeling['year_built'].apply(
            lambda year: current_year - year if pd.notnull(year) and year > 1800 else np.nan
        )
        numerical.append('property_age')
    
    # Bath to bed ratio
    if 'bathrooms' in df_modeling.columns and df_modeling['bathrooms'].notna().any() and \
       'bedrooms' in df_modeling.columns and df_modeling['bedrooms'].notna().any():
        df_modeling['bath_to_bed_ratio'] = df_modeling.apply(
            lambda row: row['bathrooms'] / row['bedrooms']
            if pd.notnull(row['bedrooms']) and row['bedrooms'] > 0 
               and pd.notnull(row['bathrooms'])
            else np.nan, axis=1
        )
        numerical.append('bath_to_bed_ratio')
    
    # Handle missing values and filter outliers for the target variable
    if 'rent_price' in df_modeling.columns and df_modeling['rent_price'].notna().any():
        rent_q1 = df_modeling['rent_price'].quantile(0.25)
        rent_q3 = df_modeling['rent_price'].quantile(0.75)
        rent_iqr = rent_q3 - rent_q1
        rent_lower = rent_q1 - 1.5 * rent_iqr
        rent_upper = rent_q3 + 1.5 * rent_iqr
        
        # Filter out extreme outliers
        df_modeling = df_modeling[
            (df_modeling['rent_price'] >= rent_lower) & 
            (df_modeling['rent_price'] <= rent_upper)
        ]
        
        print(f"Filtered to {len(df_modeling)} records after cleaning")
        
        # Split features and target
        X = df_modeling.drop(['rent_price'], axis=1, errors='ignore')
        y = df_modeling['rent_price']
    else:
        raise ValueError("Target variable 'rent_price' is missing or contains only NaN values.")
    
    # Split into train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Build preprocessing pipeline
    print("Building model pipeline...")
    
    # Clean up feature lists to include only columns with data
    categorical = [col for col in categorical if col in X.columns and X[col].notna().any()]
    numerical = [col for col in numerical if col in X.columns and X[col].notna().any()]
    
    print(f"Using {len(numerical)} numerical features and {len(categorical)} categorical features")
    
    # For numerical features
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    
    # For categorical features
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])
    
    # Combined preprocessor
    transformers = []
    if numerical:
        transformers.append(('num', numeric_transformer, numerical))
    if categorical:
        transformers.append(('cat', categorical_transformer, categorical))
    
    preprocessor = ColumnTransformer(transformers=transformers)
    
    # Create and train model
    print("Training GradientBoosting model...")
    model = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', GradientBoostingRegressor(
            n_estimators=200,
            learning_rate=0.1,
            max_depth=4,
            min_samples_split=10,
            random_state=42
        ))
    ])
    
    model.fit(X_train, y_train)
    
    # Evaluate model
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    train_r2 = r2_score(y_train, y_pred_train)
    test_r2 = r2_score(y_test, y_pred_test)
    
    print(f"Model performance:")
    print(f"- Training RMSE: ${train_rmse:.2f}")
    print(f"- Test RMSE: ${test_rmse:.2f}")
    print(f"- Training R²: {train_r2:.4f}")
    print(f"- Test R²: {test_r2:.4f}")
    
    # Compress and save model
    print("Compressing model for storage...")
    model_pickle = pickle.dumps(model)
    model_compressed = gzip.compress(model_pickle)
    model_b64 = base64.b64encode(model_compressed).decode('utf-8')
    
    # Save feature info
    feature_info = {
        'categorical': categorical,
        'numerical': numerical,
        'model_type': 'GradientBoosting',
        'compression_method': 'gzip'
    }
    
    feature_info_pickle = pickle.dumps(feature_info)
    feature_info_compressed = gzip.compress(feature_info_pickle)
    feature_info_b64 = base64.b64encode(feature_info_compressed).decode('utf-8')
    
    # Generate model version
    model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save model using SQL
    hook.run(f"""
        INSERT INTO {database}.{schema}.{model_registry_table} (
            model_version, train_rmse, test_rmse, r2, model_blob, feature_info
        ) VALUES (
            '{model_version}', {train_rmse}, {test_rmse}, {test_r2}, 
            '{model_b64}', '{feature_info_b64}'
        )
    """)
    
    print(f"Model registered with version: {model_version}")
    
    return {
        "model_version": model_version,
        "train_rmse": float(train_rmse),
        "test_rmse": float(test_rmse),
        "train_r2": float(train_r2),
        "test_r2": float(test_r2)
    }

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ml", "rent_price", "snowflake"],
)
def rent_price_prediction_training():

    @task
    def create_model_registry():
        """Create model registry table if it doesn't exist"""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # First check if table exists
        result = hook.get_first(f"""
            SELECT COUNT(*) 
            FROM {MY_DATABASE}.information_schema.tables 
            WHERE table_schema = '{MY_SCHEMA}' AND table_name = '{MODEL_REGISTRY_TABLE}'
        """)
        
        if result and result[0] == 0:
            # Create model registry table if it doesn't exist
            hook.run(f"""
                CREATE TABLE IF NOT EXISTS {MY_DATABASE}.{MY_SCHEMA}.{MODEL_REGISTRY_TABLE} (
                    model_version VARCHAR(50) PRIMARY KEY,
                    train_rmse FLOAT,
                    test_rmse FLOAT,
                    r2 FLOAT,
                    model_blob VARCHAR,
                    feature_info VARCHAR,
                    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            print("Model registry table created")
        else:
            print("Model registry table already exists")
        
        return True

    @task
    def log_model_metrics(ti=None):
        """Log the model metrics from XCom"""
        # Pull the metrics from XCom
        metrics = ti.xcom_pull(task_ids='train_rent_price_model')
        
        print(f"Model training completed successfully!")
        print(f"Model version: {metrics['model_version']}")
        print(f"Training RMSE: ${metrics['train_rmse']:.2f}")
        print(f"Test RMSE: ${metrics['test_rmse']:.2f}")
        print(f"Training R²: {metrics['train_r2']:.4f}")
        print(f"Test R²: {metrics['test_r2']:.4f}")
        return metrics

    # Define task dependencies
    create_registry_task = create_model_registry()
    
    train_model_task = PythonVirtualenvOperator(
        task_id='train_rent_price_model',
        python_callable=train_rent_price_model,
        op_kwargs={
            'snowflake_conn_id': SNOWFLAKE_CONN_ID,
            'database': MY_DATABASE,
            'schema': MY_SCHEMA,
            'feature_store_table': FEATURE_STORE_TABLE,
            'model_registry_table': MODEL_REGISTRY_TABLE
        },
        requirements=["pandas", "numpy", "scikit-learn", "snowflake-connector-python"],
        system_site_packages=True
    )
    
    log_metrics_task = log_model_metrics()
    
    # Set up the task dependencies
    create_registry_task >> train_model_task >> log_metrics_task

# Create the DAG
rent_price_prediction_dag = rent_price_prediction_training() 