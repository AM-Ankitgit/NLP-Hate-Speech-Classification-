import os

# here will write code in two different way
# 1. create bucket using console and sync your bucker from here for uploading and loading the data
# 2 .create the bucket in here using script and perform the upload and load 



class GoogleCloudSync:
    def __init__(self) -> None:
        pass

    # upload the your data to gcp
    def sync_folder_to_gcp(self,gcp_bucket_url,filepath,filename):
        command  = f"gsutil cp {filepath}/{filename} gs://{gcp_bucket_url}/"
        os.system(command)

    # download the data file from the gcp

    def sync_gcp_to_folder(self,gcp_bucket_url,filename,destination):
        command  = f"gsutil cp gs://{gcp_bucket_url}/{filename} {destination}/{filename}"
        os.system(command)




    


# Step 1: Create a GCS Bucket
# First, let’s create a new GCS bucket using Python:

def create_gcs_bucket(project_id, bucket_name, service_account_file):

    from google.cloud import storage

    """
    project_id (str): Your Google Cloud Platform (GCP) project ID.
    bucket_name (str): Name of the new bucket to be created.
    service_account_file (str): Your service account JSON key file
    """
    
    try:
        # Initialize a GCS client
        client = storage.Client.from_service_account_json(service_account_file)

        # Create a new bucket
        bucket = client.create_bucket(bucket_name)

        print(f'Bucket {bucket_name} created successfully.')
    
    except Exception as e:

        print(f'Error creating bucket: {str(e)}')
    

# Step 2: Upload an Object in Bucket
# You can upload an object using the following function by passing your ID and project credentials.

from google.cloud import storage
def upload_object_to_bucket(bucket_name, source_file, destination_blob_name, service_account_file):
    

    
    """
    bucket_name (str): Name of the GCS bucket where the object will be uploaded.
    source_file (str): Path to the local file to be uploaded.
    destination_blob_name (str): Name of the object in the GCS bucket.
    service_account_file (str): Your service account JSON key file
    """

    try:
        # Initialize a GCS client
        client = storage.Client.from_service_account_json(service_account_file)

        # Get a reference to the target GCS bucket
        bucket = client.get_bucket(bucket_name)

        # Upload the local file to GCS
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)

        print(f'File {source_file} uploaded to {bucket_name}/{destination_blob_name}')
    
    except Exception as e:
        print(f'Error uploading file: {str(e)}')

# Step 3: Retrieve an Object from Bucket
# from google.cloud import storage

def retrieve_object_from_bucket(project_id, bucket_name, object_name, destination_file_path, service_account_file):
    
    """
        project_id (str): Your Google Cloud project ID.
        bucket_name (str): The name of the GCS bucket.
        object_name (str): The name of the object you want to retrieve.
        destination_file_path (str): The path to save the retrieved object locally.
    """
    
    try:
        # Initialize a GCS client
        client = storage.Client.from_service_account_json(service_account_file)

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # Get the blob (object) from the bucket
        blob = bucket.blob(object_name)

        # Download the blob to the specified file path
        blob.download_to_filename(destination_file_path)

        print(f"Object '{object_name}' retrieved and saved to '{destination_file_path}'.")

    except Exception as e:
        print(f"Error: {e}")
