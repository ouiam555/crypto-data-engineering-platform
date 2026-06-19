import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import httpx
import json
import io
from datetime import datetime
from utils.minio_clients import get_minio_client

BUCKET_NAME = "crypto-bronze"

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1
}

response = httpx.get(url, params=params)
data = response.json()

date_str = datetime.now().strftime("%Y/%m/%d")
object_name = f"{date_str}/raw.json"

client = get_minio_client()

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

data_bytes = json.dumps(data, indent=2).encode("utf-8")

client.put_object(
    bucket_name=BUCKET_NAME,
    object_name=object_name,
    data=io.BytesIO(data_bytes),
    length=len(data_bytes),
    content_type="application/json"
)

print(f" saved: {BUCKET_NAME}/{object_name}")