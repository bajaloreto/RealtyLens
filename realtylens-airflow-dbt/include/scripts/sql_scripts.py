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

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_zoning_polygon_stage
            URL = 's3://{bucket}/zoning_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;


        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_landmarks_polygon_stage
            URL = 's3://{bucket}/landmarks_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_property_details_stage
            URL = 's3://{bucket}/property_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_zip_codes_polygon_stage
            URL = 's3://{bucket}/zip_codes_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;      
        
        """
daily_property_sql = """
            CREATE OR REPLACE TABLE DATAEXPERT_STUDENT.jmusni07.sale_listing_stg AS
        SELECT 
            f.value:id::string as sale_id,
            f.value:formattedAddress::string as formatted_address,
            f.value:addressLine1::string as address_line_1,
            f.value:addressLine2::string as address_line_2,
            f.value:city::string as city,
            f.value:state::string as state,
            f.value:zipCode::string as zip_code,
            f.value:county::string as county,
            f.value:latitude::float as latitude,
            f.value:longitude::float as longitude,
            f.value:propertyType::string as property_type,
            f.value:lotSize::decimal(10,2) as lot_size,
            f.value:status::string as status,
            'For Sale'::string as property_status,
            f.value:price::decimal(15,2) as sale_price,
            NULL as rent_price,
            f.value:listingType::string as listing_type,
            f.value:listedDate::timestamp as listed_date,
            f.value:removedDate::timestamp as removed_date,
            f.value:createdDate::timestamp as created_date,
            f.value:lastSeenDate::timestamp as last_seen_date,
            f.value:daysOnMarket::integer as days_on_market,
            f.value:mlsName::string as mls_name,
            f.value:mlsNumber::string as mls_number,
            '{ds}' as load_date,
            MD5(CONCAT(f.value:id::string, '|', 'For Sale', '|', '{ds}')) AS property_id
        FROM 
            @DATAEXPERT_STUDENT.jmusni07.aws_sale_listing_stage/PA/Philadelphia/date={ds}/listings.json,
            LATERAL FLATTEN(input => parse_json($1)) f;


        CREATE OR REPLACE TABLE DATAEXPERT_STUDENT.jmusni07.rent_listing_stg AS
        SELECT 
            f.value:id::string as rent_id,
            f.value:formattedAddress::string as formatted_address,
            f.value:addressLine1::string as address_line_1,
            f.value:addressLine2::string as address_line_2,
            f.value:city::string as city,
            f.value:state::string as state,
            f.value:zipCode::string as zip_code,
            f.value:county::string as county,
            f.value:latitude::float as latitude,
            f.value:longitude::float as longitude,
            f.value:propertyType::string as property_type,
            f.value:lotSize::decimal(10,2) as lot_size,
            f.value:status::string as status,
            'For Rent'::string as property_status,
            f.value:price::decimal(15,2) as price,
            f.value:listingType::string as listing_type,
            f.value:listedDate::timestamp as listed_date,
            f.value:removedDate::timestamp as removed_date,
            f.value:createdDate::timestamp as created_date,
            f.value:lastSeenDate::timestamp as last_seen_date,
            f.value:daysOnMarket::integer as days_on_market,
            f.value:mlsName::string as mls_name,
            f.value:mlsNumber::string as mls_number,
            '{ds}' as load_date,
            MD5(CONCAT(f.value:id::string, '|', 'For Rent', '|', '{ds}')) AS property_id
        FROM 
            @DATAEXPERT_STUDENT.jmusni07.aws_rent_listing_stage/PA/Philadelphia/date={ds}/listings.json,
            LATERAL FLATTEN(input => parse_json($1)) f;
        """



