import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

S3_ENDPOINT = "http://localhost:8333"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "key"
BUCKET_NAME = "data"

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


catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        'uri': f"postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
        "warehouse": f"s3://{BUCKET_NAME}/iceberg_catalog",

        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.region": "us-east-1" 
    },
)
df = pq.read_table("./yellow_tripdata_2023-01.parquet")

try:
    catalog.create_namespace("ok")
    print("Namespace 'ok' created.")
except NamespaceAlreadyExistsError:
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
except TableAlreadyExistsError:
    print("Table 'ok.taxi_dataset' already exists.")
    table = catalog.load_table("ok.taxi_dataset")
except Exception as e:
    print(f"An unexpected error occurred creating/loading table: {e}")
    raise


if table:
    print("Appending data to table...")
    table.append(df)
    print("Data appended successfully.")
    print(f"Total rows in table: {len(table.scan().to_arrow())}")
else:
    print("Table could not be created or loaded. Skipping append.")
