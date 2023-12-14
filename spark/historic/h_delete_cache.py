from minio import Minio
from pyspark.sql import SparkSession
import datetime


def initialize_minio_session():
    return Minio("minio:9000",
                 access_key="minioadmin",
                 secret_key="minioadmin",
                 secure=False)


def delete_all_files(minio, files_to_delete):
    bucket_name = 'datacache'
    # Delete files
    for file_name in files_to_delete:
        objects_to_delete = minio.list_objects(bucket_name, prefix=file_name, recursive=True)
        delete_object_list = [obj.object_name for obj in objects_to_delete]
        for file_to_delete in delete_object_list:
            try:
                err = minio.remove_object(bucket_name, file_to_delete)
            except Exception as e:
                print(f"Error deleting {file_name}: {e}")
                continue
            print(f"Deleted {file_name} or err: {err}")


def main():
    # List of files to delete
    files_to_delete = [
        "h_core_data.csv",
        "h_extracted_data.csv",
        "h_revenue_data.csv",
        "h_free_product_data.csv",
        "h_error_values.csv"
    ]

    minio = initialize_minio_session()
    delete_all_files(minio, files_to_delete)


if __name__ == "__main__":
    main()
