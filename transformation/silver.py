import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import json
import io
import logging
import pandas as pd
from utils.minio_clients import get_minio_client

# CONFIG
BRONZE_BUCKET = "crypto-bronze"
SILVER_BUCKET = "crypto-silver"
date_str = "2026/06/10"
object_name = f"{date_str}/raw.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = get_minio_client()

try:
    logging.info("Reading Bronze file...")

    response = client.get_object(BRONZE_BUCKET, object_name)
    data = json.loads(response.read())

    if not data:
        raise ValueError("Bronze file is empty")

    df = pd.DataFrame(data)

    required_columns = [
        "id", "name", "symbol", "market_cap_rank",
        "current_price", "high_24h", "low_24h",
        "total_volume", "market_cap",
        "price_change_24h", "price_change_percentage_24h",
        "last_updated"
    ]

    df = df[required_columns]

    # normalize
    df.columns = df.columns.str.strip().str.lower()

    # remove duplicates
    df = df.drop_duplicates()

    # remove nulls
    df = df.dropna(subset=["id", "name", "symbol", "current_price", "market_cap"])

    # numeric conversion
    numeric_cols = [
        "market_cap_rank", "current_price", "high_24h",
        "low_24h", "total_volume", "market_cap",
        "price_change_24h", "price_change_percentage_24h"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

    df = df.dropna()

    df["collection_date"] = pd.to_datetime(date_str, format="%Y/%m/%d")

    logging.info(f"Silver rows: {len(df)}")

    # save silver
    if not client.bucket_exists(SILVER_BUCKET):
        client.make_bucket(SILVER_BUCKET)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    client.put_object(
        SILVER_BUCKET,
        f"{date_str}/clean.parquet",
        buffer,
        buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    logging.info("Silver saved successfully")

except Exception as e:
    logging.error(f"Silver failed: {e}")
    raise