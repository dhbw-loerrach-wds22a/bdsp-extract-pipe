from minio import Minio
from minio.error import S3Error

def connect_to_minio():
    # Erstellen Sie ein MinIO-Client-Objekt mit Zugangsdaten
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # Setzen Sie auf True, wenn HTTPS verwendet wird
    )

    try:
        # Testen der Verbindung durch Auflisten der Buckets
        buckets = minio_client.list_buckets()
        for bucket in buckets:
            print(f"Bucket: {bucket.name}")
            # Auflisten der Objekte in jedem Bucket
            objects = minio_client.list_objects(bucket.name)
            for obj in objects:
                print(f"    Objekt: {obj.object_name}")
        print("Verbindung zu MinIO erfolgreich und Buckets aufgelistet.")
    except S3Error as e:
        print("Fehler beim Verbinden mit MinIO oder beim Auflisten der Buckets:", e)

connect_to_minio()
