from google.cloud import bigquery
from google.oauth2 import service_account
import configparser

config = configparser.ConfigParser()
config.read('configuration.properties')

project_id = config['BigQuery']['project_id']
dataset_id = config['BigQuery']['dataset_id']
table_id = config['BigQuery']['table_id_metadata']

# Authenticate using a service account key file
credentials = service_account.Credentials.from_service_account_file(
    'GCPkey.json'
)

# Initialize the BigQuery client
client = bigquery.Client(credentials=credentials, project=project_id)

# Define the schema for the new table
schema = [
    bigquery.SchemaField("file_number", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("film_number", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cik", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("filing_entity", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("form_and_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("filed_on", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("reporting_for", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("incorporated", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_extracted", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("is_summarized", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("is_in_graphdb", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("is_in_vectorized", "BOOLEAN", mode="REQUIRED"),
]

# Create a dataset (if it does not already exist)
dataset_ref = client.dataset(dataset_id)

client.get_dataset(dataset_ref)
print(f"Dataset {dataset_id} already exists.")

# Define the table reference
table_ref = dataset_ref.table(table_id)

# Create the table with the defined schema
table = bigquery.Table(table_ref, schema=schema)

try:
    table = client.create_table(table)
    print(f"Table {table_id} created in {dataset_id}.")
except Exception as e:
    print(f"An error occurred: {e}")