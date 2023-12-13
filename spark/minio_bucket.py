from minio import Minio
from minio.error import S3Error


def create_minio_bucket(bucket_name):
    # MinIO Client konfigurieren
    client = Minio("minio:9000",
                   access_key="minioadmin",
                   secret_key="minioadmin",
                   secure=False)

    # Bucket erstellen, falls er nicht existiert
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' erstellt.")
        else:
            print(f"Bucket '{bucket_name}' existiert bereits.")
    except S3Error as e:
        print(f"Fehler beim Erstellen des Buckets: {e}")


# Bucket-Namen definieren
bucket_name1 = "datacache"
bucket_name2 = "datawarehouse"

# Bucket erstellen
create_minio_bucket(bucket_name1)
create_minio_bucket(bucket_name2)
