import os
import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
# Import specific exceptions to catch
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

# --- S3 Configuration ---
S3_ENDPOINT = "http://localhost:8333"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "key"
BUCKET_NAME = "data"

os.environ["AWS_ENDPOINT_URL"] = S3_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
os.environ["AWS_SECRET_KEY"] = S3_SECRET_KEY
# This one is also important for many S3 clients
os.environ["AWS_S3_ENDPOINT"] = S3_ENDPOINT

# --- FIX 1: Set Environment Variables ---
# This forces pyarrow's S3 filesystem to use your SeaweedFS endpoint
# This MUST run before pyiceberg (or pyarrow) is used
os.environ["AWS_ENDPOINT_URL"] = S3_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
# Some S3 clients prefer this variable
os.environ["AWS_S3_ENDPOINT"] = S3_ENDPOINT
# --- End of Fix 1 ---
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# --- Create Bucket (Your existing, correct logic) ---
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created successfully.")
except ClientError as e:
    if e.response['Error']['Code'] in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
        print(f"Bucket '{BUCKET_NAME}' already exists.")
    else:
        print(f"Error creating bucket: {e}")
        raise
# --- End of Bucket Creation ---


catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        'uri': f"postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
        "warehouse": f"s3://{BUCKET_NAME}/iceberg_catalog",

        # --- KEEP ALL THESE S3 PROPERTIES ---
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.region": "us-east-1"  # Still important for v4 signing
    },
)
# Load parquet file
df = pq.read_table("./yellow_tripdata_2023-01.parquet")

# --- FIX 2: Use Specific Exceptions ---
try:
    catalog.create_namespace("ok")
    print("Namespace 'ok' created.")
except NamespaceAlreadyExistsError:  # Be specific
    print("Namespace 'ok' already exists.")
except Exception as e:
    print(f"An unexpected error occurred creating namespace: {e}")
    raise

table = None
try:
    table = catalog.create_table(
        "ok.taxi_dataset",
        schema=df.schema,
    )
    print("Table 'ok.taxi_dataset' created.")
except TableAlreadyExistsError:  # Be specific
    print("Table 'ok.taxi_dataset' already exists.")
    table = catalog.load_table("ok.taxi_dataset")
except Exception as e:
    # This will now show the *real* error (e.g., S3 issues)
    print(f"An unexpected error occurred creating/loading table: {e}")
    raise
# --- End of Fix 2 ---


# Append data
if table:
    print("Appending data to table...")
    table.append(df)
    print("Data appended successfully.")
    print(f"Total rows in table: {len(table.scan().to_arrow())}")
else:
    print("Table could not be created or loaded. Skipping append.")
