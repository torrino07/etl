import os
import json
import struct
import pandas
import numpy as np
from pathlib import Path
from subprocess import run
import numpy


# --- CONFIGURATION ---
OUTPUT_ROOT = "output_parquet"
INPUT_ROOT = "marketdata"
S3_BUCKET = "marketdata001-dev"
S3_PREFIX = "marketdata"
AWS_REGION = "us-east-1"
LEVELS = 10


# --- LOAD BINLOG FILE ---


def load_snapshots(filepath):
    snapshots = []
    with open(filepath, "rb") as f:
        while True:
            header = f.read(12)
            if len(header) < 12:
                break
            timestamp_ns, length = struct.unpack("<QI", header)
            json_bytes = f.read(length)
            message = json.loads(json_bytes.decode("utf-8"))
            message["timestamp_ns"] = timestamp_ns
            snapshots.append(message)
    return snapshots


# --- PARSE METADATA FROM PATH ---


def parse_metadata_from_path(filepath):
    p = Path(filepath)
    return {
        "channel": p.parts[1],
        "exchange": p.parts[2],
        "market": p.parts[3],
        "symbol": p.parts[4],
        "date": p.parts[5],
        "hour": p.parts[6],
    }


# --- NORMALIZERS ---


def normalize_orderbook(snap, levels=LEVELS):
    flat = {"timestamp": snap["timestamp_ns"]}
    for i in range(levels):
        flat[f"bid_price_L{i+1}"] = float(snap["bids"][i][0])
        flat[f"bid_volume_L{i+1}"] = float(snap["bids"][i][1])
        flat[f"ask_price_L{i+1}"] = float(snap["asks"][i][0])
        flat[f"ask_volume_L{i+1}"] = float(snap["asks"][i][1])
    return flat


def normalize_trades(snap):
    return {
        "eventTime": snap["eventTime"],
        "tradeId": snap["tradeId"],
        "price": float(snap["price"]),
        "quantity": float(snap["quantity"]),
        "tradeTime": snap["tradeTime"],
        "side": snap["isBuyerMaker"],
    }


# --- BUILD DATAFRAME FOR ONE FILE ---


def build_dataframe(file_path):
    meta = parse_metadata_from_path(file_path)
    raw = load_snapshots(file_path)

    if meta["channel"] == "orderbook":
        rows = [normalize_orderbook(s) for s in raw]
        df =pandas.DataFrame(rows)
        df["timestamp"] = pandas.to_datetime(df["timestamp"])
    elif meta["channel"] == "trades":
        rows = [normalize_trades(s) for s in raw]
        df =pandas.DataFrame(rows)
        df["timestamp"] = pandas.to_datetime(df["tradeTime"], unit="ms")
    else:
        raise ValueError(f"Unknown channel: {meta["channel"]}")

    for k, v in meta.items():
        if k == "hour":
            df[k] = numpy.int32(v)
        else:
            df[k] = v
    return df


# --- WRITE PARQUET ---


def write_parquet(df, output_root: str):
    channel = df["channel"].iloc[0]
    exchange = df["exchange"].iloc[0]
    market = df["market"].iloc[0]
    symbol = df["symbol"].iloc[0]
    date = df["date"].iloc[0]
    hour = df["hour"].iloc[0]

    output_path = f"{output_root}/channel={channel}/exchange={exchange}/market={market}/symbol={symbol}/date={date}/hour={hour}/"
    Path(output_path).mkdir(parents=True, exist_ok=True)

    filename = f"{symbol}-{date}-{hour}.parquet"
    full_path = output_path + filename

    df.to_parquet(full_path, index=False, compression="snappy", engine="pyarrow")
    print(f"Written: {full_path}")


# --- SYNC TO S3 ---


def sync_to_s3(local_dir, s3_bucket, s3_prefix):
    os.environ["AWS_DEFAULT_REGION"] = AWS_REGION
    result = run(
        [
            "aws",
            "s3",
            "sync",
            local_dir,
            f"s3://{s3_bucket}/{s3_prefix}/",
            "--exact-timestamps",
        ],
        capture_output=True,
        text=True,
    )
    print("S3 Sync Output:\n", result.stdout)
    if result.stderr:
        print("S3 Sync Errors:\n", result.stderr)


# --- RUN ETL FOR ONE HOUR FOLDER ---


def run_etl(hour_path: str, output_root: str):
    all_rows = []
    for minute_folder in Path(hour_path).glob("*"):
        for file_path in Path(minute_folder).glob("*"):
            df = build_dataframe(str(file_path))
            all_rows.append(df)

    if all_rows:
        df_hour =pandas.concat(all_rows, ignore_index=True)
        write_parquet(df_hour, output_root)


# --- CLEANUP ---


def clean_up(folder_path):
    run(["rm", "-rf", folder_path])
    print(f"Deleted: {folder_path}")


# --- MAIN JOB ---


def run_full_pipeline():
    hour_paths = sorted(Path(INPUT_ROOT).glob("*/" * 6))
    for hour_path in hour_paths:
        run_etl(str(hour_path), OUTPUT_ROOT)

    sync_to_s3(OUTPUT_ROOT, S3_BUCKET, S3_PREFIX)
    clean_up(OUTPUT_ROOT)


# --- CALL MAIN ---
if __name__ == "__main__":
    run_full_pipeline()
