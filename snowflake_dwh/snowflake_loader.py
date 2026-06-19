from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# ==============================
# BRONZE
# ==============================
def ingest_bronze():
    import httpx, json, io
    from datetime import datetime
    from utils.minio_clients import get_minio_client

    BUCKET_NAME = "crypto-bronze"
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}

    response = httpx.get(url, params=params)
    data = response.json()
    date_str = datetime.now().strftime("%Y/%m/%d")

    client = get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    data_bytes = json.dumps(data, indent=2).encode("utf-8")
    client.put_object(BUCKET_NAME, f"{date_str}/raw.json", io.BytesIO(data_bytes), len(data_bytes), content_type="application/json")
    print(f"Bronze saved: {date_str}")

# ==============================
# SILVER
# ==============================
def transform_silver():
    import json, io
    import pandas as pd
    from datetime import datetime
    from utils.minio_clients import get_minio_client

    date_str = datetime.now().strftime("%Y/%m/%d")
    client = get_minio_client()

    response = client.get_object("crypto-bronze", f"{date_str}/raw.json")
    data = json.loads(response.read())
    df = pd.DataFrame(data)

    required_columns = [
        "id", "name", "symbol", "market_cap_rank",
        "current_price", "high_24h", "low_24h",
        "total_volume", "market_cap",
        "price_change_24h", "price_change_percentage_24h",
        "last_updated"
    ]
    df = df[required_columns]
    df.columns = df.columns.str.strip().str.lower()
    df = df.drop_duplicates().dropna(subset=["id", "name", "symbol", "current_price", "market_cap"])

    for col in ["market_cap_rank", "current_price", "high_24h", "low_24h", "total_volume", "market_cap", "price_change_24h", "price_change_percentage_24h"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    df = df.dropna()
    df["collection_date"] = pd.to_datetime(date_str, format="%Y/%m/%d")

    if not client.bucket_exists("crypto-silver"):
        client.make_bucket("crypto-silver")

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    client.put_object("crypto-silver", f"{date_str}/clean.parquet", buffer, buffer.getbuffer().nbytes)
    print("Silver saved")

# ==============================
# GOLD
# ==============================
def build_gold():
    import io
    import pandas as pd
    from datetime import datetime
    from utils.minio_clients import get_minio_client

    date_str = datetime.now().strftime("%Y/%m/%d")
    client = get_minio_client()

    response = client.get_object("crypto-silver", f"{date_str}/clean.parquet")
    df = pd.read_parquet(io.BytesIO(response.read()))
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    dim_crypto = df[["id", "name", "symbol", "market_cap_rank"]].copy()
    dim_crypto = dim_crypto.rename(columns={"id": "crypto_id"}).drop_duplicates(subset=["crypto_id"])

    dim_date = df[["last_updated"]].copy()
    dim_date["date_id"] = dim_date["last_updated"].dt.strftime("%Y%m%d%H").astype(int)
    dim_date["full_date"] = dim_date["last_updated"].dt.date
    dim_date["year"] = dim_date["last_updated"].dt.year
    dim_date["month"] = dim_date["last_updated"].dt.month
    dim_date["week"] = dim_date["last_updated"].dt.isocalendar().week
    dim_date["day"] = dim_date["last_updated"].dt.day
    dim_date["hour"] = dim_date["last_updated"].dt.hour
    dim_date = dim_date.drop_duplicates(subset=["date_id"]).drop(columns=["last_updated"])

    fact_crypto = df.copy()
    fact_crypto["crypto_id"] = fact_crypto["id"]
    fact_crypto["date_id"] = fact_crypto["last_updated"].dt.strftime("%Y%m%d%H").astype(int)
    fact_crypto = fact_crypto[["crypto_id", "date_id", "current_price", "market_cap", "total_volume", "price_change_24h", "price_change_percentage_24h", "high_24h", "low_24h"]]

    if not client.bucket_exists("crypto-gold"):
        client.make_bucket("crypto-gold")

    def save_parquet(df, path):
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        client.put_object("crypto-gold", path, buffer, buffer.getbuffer().nbytes)

    save_parquet(dim_crypto, f"{date_str}/dim_crypto.parquet")
    save_parquet(dim_date, f"{date_str}/dim_date.parquet")
    save_parquet(fact_crypto, f"{date_str}/fact_crypto_price.parquet")
    print("Gold saved")

# ==============================
# LOAD SNOWFLAKE 
# ==============================
def load_snowflake():
    import io
    import pandas as pd
    import snowflake.connector
    from datetime import datetime
    from utils.minio_clients import get_minio_client

    date_str = datetime.now().strftime("%Y/%m/%d")
    client = get_minio_client()

    # Gold
    def read_parquet(path):
        response = client.get_object("crypto-gold", path)
        return pd.read_parquet(io.BytesIO(response.read()))

    dim_crypto = read_parquet(f"{date_str}/dim_crypto.parquet")
    dim_date = read_parquet(f"{date_str}/dim_date.parquet")
    fact_crypto = read_parquet(f"{date_str}/fact_crypto_price.parquet")

    # connexion Snowflake
    conn = snowflake.connector.connect(
        user="OUIAM",
        password="NewPassword2026!",
        account="GRRCNZI-NM20069",
        warehouse="COMPUTE_WH",
        database="CRYPTO_DW",
        schema="GOLD",
        authenticator="snowflake"
    )
    cur = conn.cursor()

    # INSERT dim_crypto
    for _, row in dim_crypto.iterrows():
        cur.execute("""
            MERGE INTO DIM_CRYPTO t
            USING (SELECT %s AS crypto_id) s
            ON t.crypto_id = s.crypto_id
            WHEN NOT MATCHED THEN
            INSERT (crypto_id, name, symbol, market_cap_rank)
            VALUES (%s, %s, %s, %s)
        """, (row.crypto_id, row.crypto_id, row.name, row.symbol, int(row.market_cap_rank)))

    # INSERT dim_date
    for _, row in dim_date.iterrows():
        cur.execute("""
            MERGE INTO DIM_DATE t
            USING (SELECT %s AS date_id) s
            ON t.date_id = s.date_id
            WHEN NOT MATCHED THEN
            INSERT (date_id, full_date, year, month, week, day, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (int(row.date_id), int(row.date_id), str(row.full_date),
              int(row.year), int(row.month), int(row.week), int(row.day), int(row.hour)))

    # INSERT fact
    for _, row in fact_crypto.iterrows():
        cur.execute("""
            INSERT INTO FACT_CRYPTO_PRICE
            (crypto_id, date_id, current_price, market_cap, total_volume,
             price_change_24h, price_change_percentage_24h, high_24h, low_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row.crypto_id, int(row.date_id), float(row.current_price),
              float(row.market_cap), float(row.total_volume),
              float(row.price_change_24h), float(row.price_change_percentage_24h),
              float(row.high_24h), float(row.low_24h)))

    conn.commit()
    cur.close()
    conn.close()
    print("Snowflake loaded successfully")

# ==============================
# DAG
# ==============================
with DAG(
    dag_id="crypto_pipeline_dag",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    t1 = PythonOperator(task_id="ingest_bronze", python_callable=ingest_bronze)
    t2 = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    t3 = PythonOperator(task_id="build_gold", python_callable=build_gold)
    t4 = PythonOperator(task_id="load_snowflake", python_callable=load_snowflake)

    t1 >> t2 >> t3 >> t4