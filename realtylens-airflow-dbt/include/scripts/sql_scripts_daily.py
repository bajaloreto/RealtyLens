stages_sql = """
        CREATE FILE FORMAT IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.json_format 
            TYPE = JSON;
        CREATE FILE FORMAT IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.csv_format 
            TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        
        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_sale_listing_stage
            URL = 's3://{bucket}/sales_listing/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_rent_listing_stage
            URL = 's3://{bucket}/rental_listing/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;
  
        """

refresh_stages_sql = """
        ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_sale_listing_stage REFRESH;
        ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_rent_listing_stage REFRESH;
        """

daily_property_sql = """

        -- Create cumulative_rent_listing
        CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.cumulative_rent_listing (
        PROPERTY_ID VARCHAR,
        first_listed_date DATE,
        last_active_date DATE,
        date DATE,
        RENT_PRICE FLOAT,
        days_on_market INTEGER,
        property_state VARCHAR,
        price_state VARCHAR,
        PRIMARY KEY (PROPERTY_ID, date)
        );

        -- Create cumulative_sale_listing
        CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.cumulative_sale_listing (
        PROPERTY_ID VARCHAR,
        first_listed_date DATE,
        last_active_date DATE,
        date DATE,
        SALE_PRICE FLOAT,
        days_on_market INTEGER,
        property_state VARCHAR,
        price_state VARCHAR,
        PRIMARY KEY (PROPERTY_ID, date)
        );



    CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.FCT_RENT_LISTING (
        listing_sk VARCHAR(255) PRIMARY KEY,
        listing_id VARCHAR(255) NOT NULL,
        property_sk VARCHAR(255),
        status_sk VARCHAR(255),
        location_sk VARCHAR(255),
        mls_sk VARCHAR(255),
        load_date_sk DATE,
        listed_date_sk DATE,
        removed_date_sk DATE,
        created_date_sk DATE,
        last_seen_date_sk DATE,
        rent_price FLOAT,
        days_on_market INTEGER,
        property_status VARCHAR(50),
        status VARCHAR(50),
        listing_type VARCHAR(50),
        load_date VARCHAR(50),
        etl_timestamp TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.FCT_SALE_LISTING (
        listing_sk VARCHAR(255) PRIMARY KEY,
        listing_id VARCHAR(255) NOT NULL,
        property_sk VARCHAR(255),
        status_sk VARCHAR(255),
        location_sk VARCHAR(255),
        mls_sk VARCHAR(255),
        load_date_sk DATE,
        listed_date_sk DATE,
        removed_date_sk DATE,
        created_date_sk DATE,
        last_seen_date_sk DATE,
        sale_price FLOAT,
        days_on_market INTEGER,
        property_status VARCHAR(50),
        status VARCHAR(50),
        listing_type VARCHAR(50),
        load_date VARCHAR(50),
        etl_timestamp TIMESTAMP
    );        


        CREATE OR REPLACE TABLE DATAEXPERT_STUDENT.jmusni07.raw_daily_sale_listing AS
        SELECT 
            f.value:id::string as id,
            f.value:formattedAddress::string as formattedAddress,
            f.value:addressLine1::string as addressLine1,
            f.value:addressLine2::string as addressLine2,
            f.value:city::string as city,
            f.value:state::string as state,
            f.value:zipCode::string as zipCode,
            f.value:county::string as county,
            f.value:latitude::float as latitude,
            f.value:longitude::float as longitude,
            f.value:propertyType::string as propertyType,
            f.value:lotSize::decimal(10,2) as lotSize,
            f.value:status::string as status,
            f.value:price::decimal(15,2) as price,
            f.value:listingType::string as listingType,
            f.value:listedDate::timestamp as listedDate,
            f.value:removedDate::timestamp as removedDate,
            f.value:createdDate::timestamp as createdDate,
            f.value:lastSeenDate::timestamp as lastSeenDate,
            f.value:daysOnMarket::integer as daysOnMarket,
            f.value:mlsName::string as mlsName,
            f.value:mlsNumber::string as mlsNumber,
            '{ds}' as load_date
        FROM 
            @DATAEXPERT_STUDENT.jmusni07.aws_sale_listing_stage/PA/Philadelphia/date={ds}/listings.json,
            LATERAL FLATTEN(input => parse_json($1)) f;


        CREATE OR REPLACE TABLE DATAEXPERT_STUDENT.jmusni07.raw_daily_rent_listing AS
        SELECT 
            f.value:id::string as id,
            f.value:formattedAddress::string as formattedAddress,
            f.value:addressLine1::string as addressLine1,
            f.value:addressLine2::string as addressLine2,
            f.value:city::string as city,
            f.value:state::string as state,
            f.value:zipCode::string as zipCode,
            f.value:county::string as county,
            f.value:latitude::float as latitude,
            f.value:longitude::float as longitude,
            f.value:propertyType::string as propertyType,
            f.value:lotSize::decimal(10,2) as lotSize,
            f.value:status::string as status,
            f.value:price::decimal(15,2) as price,
            f.value:listingType::string as listingType,
            f.value:listedDate::timestamp as listedDate,
            f.value:removedDate::timestamp as removedDate,
            f.value:createdDate::timestamp as createdDate,
            f.value:lastSeenDate::timestamp as lastSeenDate,
            f.value:daysOnMarket::integer as daysOnMarket,
            f.value:mlsName::string as mlsName,
            f.value:mlsNumber::string as mlsNumber,
            '{ds}' as load_date
        FROM 
            @DATAEXPERT_STUDENT.jmusni07.aws_rent_listing_stage/PA/Philadelphia/date={ds}/listings.json,
            LATERAL FLATTEN(input => parse_json($1)) f;


        """



