import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
import time
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Get Klaviyo API key from environment variable
KLAVIYO_API_KEY = os.environ.get('KKEY')
if not KLAVIYO_API_KEY:
    raise ValueError('Klaviyo API key not found in environment variables.')

# Service account JSON file in the same directory as main.py
SERVICE_ACCOUNT_FILE = 'service_account.json'  

# Your Google Cloud project ID
PROJECT_ID = 'proj-cdp-demo'  # Replace with your project ID

# Create credentials and BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# BigQuery dataset and table
DATASET_ID = os.environ.get('DATASET_ID')      
PROJECT_ID = os.environ.get('PROJECT_ID')
TABLE_ID = 'klaviyo_campaign_tags'         # Replace with your table name
FULL_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'

def fetch_campaign_ids_and_tags(api_key, channel='email', limit=10):
    url = 'https://a.klaviyo.com/api/campaigns/'
    headers = {
        'Authorization': f'Klaviyo-API-Key {api_key}',
        'Accept': 'application/json',
        'Revision': '2024-07-15'  # Latest revision date
    }
    campaign_ids_and_tags = []
    has_next = True
    page_cursor = None

    while has_next and len(campaign_ids_and_tags) < limit:
        # Include tags and request specific fields for tags
        params = {
            'filter': f'equals(messages.channel,"{channel}")',
            'fields[tag]': 'name',  # Request the tag name
            'include': 'tags'  # Include tags in the response
        }
        if page_cursor:
            params['page[cursor]'] = page_cursor

        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
            continue
        elif response.status_code != 200:
            raise Exception(f'Error fetching campaigns: {response.text}')
        
        data = response.json()
        if 'data' in data:
            for campaign in data['data']:
                campaign_id = campaign['id']
                campaign_name = campaign['attributes']['name']
                # Extract tags from the relationships
                tags_data = campaign.get('relationships', {}).get('tags', {}).get('data', [])
                tags = []
                for tag in tags_data:
                    tag_id = tag['id']
                    # Extract the tag name from the included section
                    tag_name = next(
                        (included['attributes']['name'] for included in data.get('included', [])
                         if included['type'] == 'tag' and included['id'] == tag_id), None)
                    tags.append({
                        'tag_id': tag_id,
                        'tag_name': tag_name
                    })

                campaign_ids_and_tags.append({
                    'campaign_id': campaign_id,
                    'campaign_name': campaign_name,
                    'tags': tags
                })
                
                if len(campaign_ids_and_tags) >= limit:
                    break
        else:
            logging.warning(f"No campaigns found in response: {response.text}")
        
        # Pagination: get next cursor if available
        next_link = data.get('links', {}).get('next')
        page_cursor = None
        if next_link:
            parsed_url = urllib.parse.urlparse(next_link)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            page_cursor = query_params.get('page[cursor]', [None])[0]
        
        has_next = bool(page_cursor)

    return campaign_ids_and_tags

def main():
    # Fetch campaign IDs and tags (limit to 10 campaigns for test run)
    logging.info('Fetching campaign IDs and tags...')
    campaign_list = fetch_campaign_ids_and_tags(KLAVIYO_API_KEY, limit=1000)
    logging.info(f"Fetched {len(campaign_list)} campaigns with tags.")

    # Flatten the data to create a list of campaign-tag relationships
    all_campaign_tags = []
    for campaign in campaign_list:
        campaign_id = campaign['campaign_id']
        campaign_name = campaign['campaign_name']
        tags = campaign['tags']
        for tag in tags:
            all_campaign_tags.append({
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'tag_id': tag['tag_id'],
                'tag_name': tag['tag_name']
            })

    logging.info(f"Fetched {len(all_campaign_tags)} campaign-tag relationships.")

    # Create DataFrame
    if all_campaign_tags:
        df_campaign_tags = pd.DataFrame(all_campaign_tags)
    else:
        logging.warning('No campaign tags fetched.')
        df_campaign_tags = pd.DataFrame(columns=['campaign_id', 'campaign_name', 'tag_id', 'tag_name'])

    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('campaign_id', 'STRING'),
            bigquery.SchemaField('campaign_name', 'STRING'),
            bigquery.SchemaField('tag_id', 'STRING'),
            bigquery.SchemaField('tag_name', 'STRING'),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table each time
    )

    logging.info(f"Loading data into BigQuery table {FULL_TABLE_ID}...")
    job = client.load_table_from_dataframe(
        df_campaign_tags, FULL_TABLE_ID, job_config=job_config
    )
    job.result()  # Wait for the job to complete

    logging.info(f"Loaded {len(df_campaign_tags)} rows into {FULL_TABLE_ID}.")

if __name__ == '__main__':
    main()
