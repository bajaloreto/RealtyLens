from airflow.models import Variable
import boto3

def check_existing_data(ds, task_instance):
    """Check if data already exists for today's date"""
    try:
        print("Attempting to retrieve AWS variables...")
        
        access_key = Variable.get('AWS_ACCESS_KEY_ID')
        secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')
        region = Variable.get('AWS_DEFAULT_REGION')

        if not all([access_key, secret_key, region]):
            raise ValueError("Missing required AWS credentials")
        
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        bucket = 'raw-property-data-jem'
        
        sales_prefix = f"sales_listing/PA/Philadelphia/date={ds}"
        rental_prefix = f"rental_listing/PA/Philadelphia/date={ds}"
        
        sales_objects = s3.list_objects_v2(Bucket=bucket, Prefix=sales_prefix)
        rental_objects = s3.list_objects_v2(Bucket=bucket, Prefix=rental_prefix)
        
        if 'Contents' in sales_objects and 'Contents' in rental_objects:
            print(f"Data already exists for {ds}")
            task_instance.xcom_push(key='extract_date', value=ds)
            return 'create_schema'
        else:
            print(f"No existing data found for {ds}")
            return 'extract_data'
            
    except Exception as e:
        print(f"Error in check_existing_data: {str(e)}")
        raise