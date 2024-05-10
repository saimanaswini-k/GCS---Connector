from google.oauth2 import service_account
from google.cloud import storage
from uuid import uuid4
from pyspark.sql import DataFrame
from typing import List,Dict
from models.object_info import CustomMetadata,ObjectInfo
from pyspark.sql import SparkSession

google_credentials=service_account.Credentials.from_service_account_file("/root/cloud/demo/key.json")

class GCS():
    def __init__(self,connector_config) -> None:
        self.bucket = connector_config.get('bucket',None)
        self.connector_config=connector_config
        self.prefix = connector_config.get("prefix", "/")
        self.obj_prefix = f"gs://{self.bucket}/"
        self.gcs_client = self._get_client() 


    def _get_client(self):
        client = storage.Client(credentials=google_credentials)
        try:
            return client
        except Exception as e:
            raise Exception(f"Error creating GCS client: {str(e)}") from e

    def _get_spark_session(self):
            """
            Initialize and configure SparkSession for reading from Google Cloud Storage (GCS).
            """
            app_name = "Read from GCS"
            jar_path = "/root/spark/jars/gcs-connector-hadoop3-2.2.22-shaded.jar" # referece from maven
            keyfile_path = "/root/Cloud/gcs/key.json" # read from __init__ args
            
            # Initialize SparkSession
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars", jar_path) \
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile_path) \
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                .getOrCreate()
            
            return spark


    def fetch_metadata(gcs_client, bucket_name: str, object_name: str) -> List[CustomMetadata]:
        try:
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            if not blob.exists():
                print(f"Object '{object_name}' not found in bucket '{bucket_name}'.")
                return []
            
            # Fetch metadata from the object
            blob.reload()
            metadata = blob.metadata

            if metadata:
                metadata_list = [[key, value] for key, value in metadata.items()]
                print(f"Metadata fetched for object '{object_name}':{metadata_list}")
            else:
                print(f"No metadata found for object '{object_name}'.")
                return []
        except Exception as e:
            print(f"Error fetching metadata: {str(e)}")
            return []
        

        

    def update_metadata(gcs_client, bucket: str, object_name: str, metadata: list) -> bool:

        try:
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Check if the object exists
            if not blob.exists():
                print(f"Object '{object_name}' not found in bucket '{bucket_name}'.")
                return False
            
            # Fetch existing metadata
            blob.reload()
            existing_metadata = blob.metadata
            
            # Check if the key already exists in the existing metadata
            for item in metadata:
                key = item["key"]
                value = item["value"]
                if key in existing_metadata:
                    print(f"Key '{key}' already exists in existing metadata for object '{object_name}'.")
                    return False
                existing_metadata[key] = value
            
            # Update metadata for the object
            blob.metadata = existing_metadata
            blob.patch()
            
            print(f"Metadata updated for object '{object_name}'.")
            return True
        except Exception as e:
            print(f"Error updating metadata: {str(e)}")
            return False
     




    def fetch_objects(self) -> List[ObjectInfo]:
        try:
            objects = self.gcs_client.list_blobs(self.bucket)
            if not objects:
                print(f"No objects found")
                return []
            objects_info = []
            for obj in objects:
                object_info = ObjectInfo(
                    id=str(uuid4()),
                    bucket_name=self.bucket,
                    location=f"{self.obj_prefix}{obj.name}",  # Accessing blob's name
                    format=obj.name.split(".")[-1],  # Extracting format from the name
                    file_size_kb=obj.size // 1024,
                    file_hash=obj.etag.strip('"')
                )
            objects_info.append(object_info.to_dict())
            print(objects_info)  # Move print statement outside the loop
            return objects_info
        except Exception as e:
            print(f"Error fetching objects: {str(e)}")
            return []







    def _read_objects(self, object_path: str, spark: SparkSession, file_format: str) -> DataFrame:
        try:
            # Determine file format based on the provided parameter
            if file_format == "jsonl":
                df = spark.read.format("json").load(f"gs://{self.bucket}/{object_path}")
            elif file_format == "json":
                df = spark.read.format("json").option("multiLine", True).load(f"gs://{self.bucket}/{object_path}")
            elif file_format == "json.gz":
                df = spark.read.format("json").option("compression", "gzip").option("multiLine", True).load(f"gs://{self.bucket}/{object_path}")
            elif file_format == "csv":
                df = spark.read.format("csv").option("header", True).load(f"gs://{self.bucket}/{object_path}")
            elif file_format == "csv.gz":
                df = spark.read.format("csv").option("header", True).option("compression", "gzip").load(f"gs://{self.bucket}/{object_path}")
            else:
                raise Exception(f"Unsupported file format for file: {file_format}")
            
            print(f"Object '{object_path}' read successfully from bucket '{self.bucket}' with file format '{file_format}'.")
            return df
        except Exception as e:
            print(f"Error reading object: {str(e)}")
            return None



    def _list_objects(self, bucket: str) -> list:
        summaries = []
        api_calls = 0
        try:
            print(f"Bucket: {bucket}, Prefix: {self.prefix}")
            bucket_obj = self.gcs_client.bucket(bucket)
            blobs = bucket_obj.list_blobs(prefix=self.prefix if self.prefix != '/' else '')
            for page in blobs.pages:
                api_calls += 1
                # Extract blob names from the current page
            for blob in page:
                summaries.append(blob.name)
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            print(f"API calls made: {api_calls}")
        return summaries, api_calls

bucket_name = "unicharm_bucket"
object_path = "/v1/data.json"   
object_name="tokenizer_config.json"   
gcs_connector = GCS({"bucket": bucket_name})
spark = gcs_connector._get_spark_session()

labels = [
        {"key": "request_method", "value": "PUT"},
        {"key": "method_name", "value": "setObjectTagging"},
        {"key": "object_", "value": object_path }
    ]
gcs_connector = GCS({"bucket": bucket_name})

# file_forma

# # Fetch custom metadata
metadata = GCS.fetch_metadata(gcs_connector.gcs_client, bucket_name, object_name)


# #Update custom metadata
success = GCS.update_metadata(gcs_connector.gcs_client, bucket_name, object_name, labels)


# Call the fetch_objects method on the instance
objects_info = gcs_connector.fetch_objects()


# # Example of retrieving a specific object
df = gcs_connector._read_objects(object_name,  spark,"json")  # Provide file format here
if df:
    df.show(truncate=False)
else:
    print("Error: Object not found.")

# #Listing all objects
summaries, api_calls = gcs_connector._list_objects(gcs_connector.bucket)
print(f"Retrieved {len(summaries)} objects in {api_calls} API calls.")
for summary in summaries:
         print(summary)  # Access object name
