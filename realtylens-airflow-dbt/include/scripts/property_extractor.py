import requests
import json
import boto3
import time
from airflow.models import Variable

class RentcastExtractor:
   def __init__(self, api_key, access_key, secret_key, region):
       self.api_key = api_key
       self.s3 = boto3.client(
           's3',
           aws_access_key_id=access_key,
           aws_secret_access_key=secret_key,
           region_name=region
       )
       self.bucket = 'raw-property-data-jem'
       print(f"Initialized RentcastExtractor with API key: {self.api_key[:8]}...")

   def fetch_listings(self, endpoint_type, state, city, max_calls=20):
       base_url = f"https://api.rentcast.io/v1/listings/{endpoint_type}"
       headers = {
           "accept": "application/json",
           "X-Api-Key": self.api_key
       }
       
       params = {
           "city": city,
           "state": state,
           "status": "Active",
           "limit": 500,
           "offset": 0
       }
       
       all_listings = []
       api_calls = 0
       
       while api_calls < max_calls:
           try:
               print(f"\nMaking request to {endpoint_type} endpoint with offset: {params['offset']}")
               response = requests.get(base_url, headers=headers, params=params)
               api_calls += 1
               
               if response.status_code != 200:
                   print(f"API Error: {response.status_code}")
                   print(f"Response: {response.text}")
                   print(f"URL: {base_url}")
                   print(f"Params: {params}")
                   safe_headers = headers.copy()
                   safe_headers['X-Api-Key'] = safe_headers['X-Api-Key'][:8] + '...'
                   print(f"Headers: {safe_headers}")
                   break
               
               data = response.json()
               listings = data if isinstance(data, list) else data.get('listings', [])
               
               if not listings:
                   print("No more listings found")
                   break
                   
               all_listings.extend(listings)
               print(f"Fetched {len(listings)} listings. Total: {len(all_listings)}")
               
               if len(listings) < params['limit']:
                   print("Reached last page")
                   break
                   
               params['offset'] += params['limit']
               time.sleep(1)
               
           except Exception as e:
               print(f"Error occurred: {str(e)}")
               break
       
       return all_listings

   def save_to_s3(self, listings, listing_type, state, city, extract_date):
       prefix = f"{listing_type}_listing/{state}/{city}/date={extract_date}"
       key = f"{prefix}/listings.json"
       
       try:
           self.s3.put_object(
               Bucket=self.bucket,
               Key=key,
               Body=json.dumps(listings, indent=2)
           )
           s3_path = f"s3://{self.bucket}/{key}"
           print(f"Successfully saved {len(listings)} listings to {s3_path}")
           return s3_path
       except Exception as e:
           print(f"Error saving to S3: {str(e)}")
           return None

   def run_extraction(self, state, city, extract_date):
       results = {}
       
       print("\nStarting sales listings extraction...")
       sales_listings = self.fetch_listings('sale', state, city)
       if sales_listings:
           results['sales_path'] = self.save_to_s3(sales_listings, 'sales', state, city, extract_date)
       
       print("\nStarting rental listings extraction...")
       rental_listings = self.fetch_listings('rental/long-term', state, city)
       if rental_listings:
           results['rental_path'] = self.save_to_s3(rental_listings, 'rental', state, city, extract_date)
       
       return results

def extract_property_data(ds):
   try:
       print("Attempting to retrieve all variables...")
       
       api_key = Variable.get('RENTCAST_API_KEY')
       access_key = Variable.get('AWS_ACCESS_KEY_ID')
       secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')
       region = Variable.get('AWS_DEFAULT_REGION')

       if not all([api_key, access_key, secret_key, region]):
           raise ValueError("Missing required credentials")

       print("Successfully retrieved all required variables")
       
       
       extractor = RentcastExtractor(
           api_key=api_key,
           access_key=access_key,
           secret_key=secret_key,
           region=region,
       )
       
       results = extractor.run_extraction("PA", "Philadelphia", ds)
       if not results:
           raise Exception("Extraction returned no results")
           
       return results
       
   except Exception as e:
       print(f"Error in extraction task: {str(e)}")
       print(f"Exception type: {type(e)}")
       import traceback
       print(f"Traceback: {traceback.format_exc()}")
       raise