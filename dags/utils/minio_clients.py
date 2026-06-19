from minio import Minio

def get_minio_client():
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )
