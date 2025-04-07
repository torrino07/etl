import os, json, struct, pandas
import subprocess
from pathlib import Path
from utils.DataParser import normalize_orderbook, normalize_trades

LOCAL_DIR = "binlogs"
PREFIX = "binlogs"
AWS_REGION = "us-east-1"
HOST = "marketdata001-dev"
OUTPUT_DIR = "parquet"
LEVELS = 10

def aws_sync_s3(local_dir, host, prefix):
    os.environ["AWS_DEFAULT_REGION"] = AWS_REGION
    result = subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            f"s3://{host}/{prefix}",
            local_dir,
            "--exact-timestamps",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print("Sync successful")
    else:
        print("Sync failed")
        print(result.stderr)

def parse_metadata(path):
    parts = Path(path).parts
    return {
        "channel": parts[1],
        "exchange": parts[2],
        "market": parts[3],
        "symbol": parts[4],
        "date": parts[5],
        "hour": parts[6],
        "minute": parts[7],
    }

def load_snapshots(path):
    snapshots = []
    with open(path, "rb") as f:
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

def write_parquet_append(df, host, output_root):
    df.to_parquet(
        f"s3://{host}/{output_root}", 
        partition_cols=["channel", "exchange", "market", "symbol", "date", "hour", "minute"],
        index=False,
        compression="snappy",
        engine="pyarrow"
        )
    print(f"Appended file: {output_root}")

def clean_up(path):
    subprocess.run(["rm", "-rf", path])
    print(f"Deleted: {path}")
    
def run(channel, local_dir=LOCAL_DIR, host=HOST, prefix=PREFIX, output_root=OUTPUT_DIR):
    
    aws_sync_s3(local_dir, host, prefix)
    paths = sorted((Path(local_dir + f"/{channel}").rglob("*.binlog")))

    all_dfs = []
    for path in paths:
        meta = parse_metadata(path)
        snapshots = load_snapshots(path)
        
        if meta["channel"] == "orderbook":
            rows = [normalize_orderbook(s, levels=LEVELS) for s in snapshots]
            df = pandas.DataFrame(rows)
            df["timestamp"] = pandas.to_datetime(df["timestamp"])
        elif meta["channel"] == "trades":
            rows = [normalize_trades(s) for s in snapshots]
            df = pandas.DataFrame(rows)
            df["timestamp"] = pandas.to_datetime(df["tradeTime"], unit="ms")
        else:
            raise ValueError(f"Unknown channel: {meta["channel"]}")
        
        for k, v in meta.items():
            df[k] = v

        all_dfs.append(df)
        
    if all_dfs:
        dfs = pandas.concat(all_dfs, ignore_index=True)
        write_parquet_append(dfs, host, output_root)


if __name__ == "__main__":
    run(channel="trades", local_dir=LOCAL_DIR, host=HOST, prefix=PREFIX, output_root=OUTPUT_DIR)
    run(channel="oderbook", local_dir=LOCAL_DIR, host=HOST, prefix=PREFIX, output_root=OUTPUT_DIR)
    clean_up(path=LOCAL_DIR)