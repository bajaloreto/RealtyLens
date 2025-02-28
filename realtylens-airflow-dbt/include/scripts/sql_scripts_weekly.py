create_stages_sql = """
        CREATE FILE FORMAT IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.json_format 
            TYPE = JSON;
        CREATE FILE FORMAT IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.csv_format 
            TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        
        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_zoning_polygon_stage
            URL = 's3://{bucket}/zoning_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;


        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_landmarks_polygon_stage
            URL = 's3://{bucket}/landmarks_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_zip_codes_polygon_stage
            URL = 's3://{bucket}/zip_codes_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;      

        CREATE STAGE IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07.aws_property_details_stage
            URL = 's3://{bucket}/property_data/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = json_format;      
"""

refresh_stages_sql = """
    ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_zoning_polygon_stage REFRESH;
    ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_landmarks_polygon_stage REFRESH;
    ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_zip_codes_polygon_stage REFRESH;
    ALTER STAGE DATAEXPERT_STUDENT.jmusni07.aws_property_details_stage REFRESH;
"""



raw_data_load_sql = """
CREATE OR REPLACE TABLE dataexpert_student.jmusni07.raw_zipcode_polygon AS
SELECT 
  f.value:properties:OBJECTID as OBJECTID,
  f.value:properties:CODE as CODE,
  f.value:properties:COD as COD,
  f.value:properties:Shape__Area as Shape__Area,
  f.value:properties:Shape__Length as Shape__Length,
  f.value:geometry:type as geometry_type,
  f.value:geometry as geometry_json,
  TO_DATE('{ds}') as load_date
FROM @DATAEXPERT_STUDENT.jmusni07.aws_zip_codes_polygon_stage,
LATERAL FLATTEN(input => $1:features) f;


CREATE OR REPLACE TABLE dataexpert_student.jmusni07.raw_zoning_polygon AS
SELECT 
  f.value:properties:OBJECTID as OBJECTID,
  f.value:properties:CODE as CODE,
  f.value:properties:LONG_CODE as LONG_CODE,
  f.value:properties:ZONINGGROUP as ZONINGGROUP,
  f.value:geometry:type as geometry_type,
  f.value:geometry as geometry_json,
  TO_DATE('{ds}') as load_date
FROM @DATAEXPERT_STUDENT.jmusni07.aws_zoning_polygon_stage,
LATERAL FLATTEN(input => $1:features) f;


CREATE OR REPLACE TABLE dataexpert_student.jmusni07.raw_landmark_polygon AS
SELECT 
  f.value:properties:OBJECTID as OBJECTID,
  f.value:properties:NAME as NAME,
  f.value:properties:ADDRESS as ADDRESS,
  f.value:properties:FEAT_TYPE as FEAT_TYPE,
  f.value:properties:SUB_TYPE as SUB_TYPE,
  f.value:properties:VANITY_NAME as VANITY_NAME,
  f.value:properties:SECONDARY_NAME as SECONDARY_NAME,
  f.value:properties:BLDG as BLDG,
  f.value:properties:PARENT_NAME as PARENT_NAME,
  f.value:properties:PARENT_TYPE as PARENT_TYPE,
  f.value:properties:ACREAGE as ACREAGE,
  f.value:properties:PARENT_ACREAGE as PARENT_ACREAGE,
  f.value:properties:Shape__Area as Shape__Area,
  f.value:properties:Shape__Length as Shape__Length,
  f.value:geometry:type as geometry_type,
  f.value:geometry as geometry_json,
  TO_DATE('{ds}') as load_date
FROM @DATAEXPERT_STUDENT.jmusni07.aws_landmarks_polygon_stage,
LATERAL FLATTEN(input => $1:features) f;



CREATE OR REPLACE TABLE DATAEXPERT_STUDENT.jmusni07.raw_property_details AS
SELECT 
    f.value:id as id,
    f.value:formattedAddress as formattedAddress,
    f.value:addressLine1 as addressLine1,
    f.value:addressLine2 as addressLine2,
    f.value:city as city,
    f.value:state as state,
    f.value:zipCode as zipCode,
    f.value:county as county,
    f.value:latitude as latitude,
    f.value:longitude as longitude,
    f.value:propertyType as propertyType,
    f.value:bedrooms as bedrooms,
    f.value:bathrooms as bathrooms,
    f.value:squareFootage as squareFootage,
    f.value:lotSize as lotSize,
    f.value:yearBuilt as yearBuilt,
    f.value:lastSaleDate as lastSaleDate,
    f.value:lastSalePrice as lastSalePrice,
    TO_DATE('{ds}') as load_date
FROM 
    @DATAEXPERT_STUDENT.jmusni07.aws_property_details_stage,
    LATERAL FLATTEN(input => parse_json($1)) f;
"""