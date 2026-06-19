import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import io
import logging
import pandas as pd
from utils.minio_clients import get_minio_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

SILVER_BUCKET = "crypto-silver"
GOLD_BUCKET = "crypto-gold"
date_str = "2026/06/10"

client = get_minio_client()

# -----------------------
# LOAD SILVER
# -----------------------
silver_object = f"{date_str}/clean.parquet"
response = client.get_object(SILVER_BUCKET, silver_object)
df = pd.read_parquet(io.BytesIO(response.read()))

logging.info(f"Silver loaded: {df.shape}")

df["last_updated"] = pd.to_datetime(df["last_updated"])

# DIM CRYPTO

dim_crypto = df[["id", "name", "symbol", "market_cap_rank"]].copy()
dim_crypto = dim_crypto.rename(columns={"id": "crypto_id"})

dim_crypto = dim_crypto.dropna(subset=["crypto_id"])
dim_crypto = dim_crypto.drop_duplicates(subset=["crypto_id"])

logging.info(f"dim_crypto: {dim_crypto.shape}")

#  DIM DATE
dim_date = df[["last_updated"]].copy()

dim_date["date_id"] = dim_date["last_updated"].dt.strftime("%Y%m%d%H").astype(int)
dim_date["full_date"] = dim_date["last_updated"].dt.date
dim_date["year"] = dim_date["last_updated"].dt.year
dim_date["month"] = dim_date["last_updated"].dt.month
dim_date["week"] = dim_date["last_updated"].dt.isocalendar().week
dim_date["day"] = dim_date["last_updated"].dt.day
dim_date["hour"] = dim_date["last_updated"].dt.hour

dim_date = dim_date.drop_duplicates(subset=["date_id"])
dim_date = dim_date.drop(columns=["last_updated"]).reset_index(drop=True)

logging.info(f"dim_date: {dim_date.shape}")

#  FACT TABLE (IMPORTANT)
fact_crypto = df.copy()

fact_crypto["crypto_id"] = fact_crypto["id"]
fact_crypto["date_id"] = fact_crypto["last_updated"].dt.strftime("%Y%m%d%H").astype(int)

fact_crypto = fact_crypto[[
    "crypto_id",
    "date_id",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_24h",
    "price_change_percentage_24h",
    "high_24h",
    "low_24h"
]]

logging.info(f"fact_crypto: {fact_crypto.shape}")

# SAVE TO MINIO
def save_parquet(df, path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    client.put_object(
        GOLD_BUCKET,
        path,
        buffer,
        buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

if not client.bucket_exists(GOLD_BUCKET):
    client.make_bucket(GOLD_BUCKET)

save_parquet(dim_crypto, f"{date_str}/dim_crypto.parquet")
save_parquet(dim_date, f"{date_str}/dim_date.parquet")
save_parquet(fact_crypto, f"{date_str}/fact_crypto_price.parquet")

logging.info("Gold layer saved successfully (STAR SCHEMA READY)")