from minio import Minio
import datetime
import re

def initialize_minio_session():
    return Minio("minio:9000",
                 access_key="minioadmin",
                 secret_key="minioadmin",
                 secure=False)

def extract_date_from_filename(filename):
    # This pattern matches dates in the format YYYY-MM-DD at the end of the filename
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    return datetime.datetime.strptime(match.group(), '%Y-%m-%d').date() if match else None

def delete_old_files(minio, bucket_name, days_old):
    try:
        objects = minio.list_objects(bucket_name, recursive=True)
        today = datetime.date.today()
        for obj in objects:
            file_date = extract_date_from_filename(obj.object_name)
            if file_date and (today - file_date).days >= days_old:
                minio.remove_object(bucket_name, obj.object_name)
                print(f"Deleted {obj.object_name}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    bucket_name = 'datacache'
    days_old = 0  # Delete files older than 30 days

    minio = initialize_minio_session()
    delete_old_files(minio, bucket_name, days_old)

if __name__ == "__main__":
    main()
