import os, json, struct, pandas, boto3
from pathlib import Path
from utils.DataParser import normalize_orderbook, normalize_trades

def aws_sync_s3():
    response = os.system(f"aws s3 sync s3://{S3_BUCKET}/binlogs/staging ./binlogs --exact-timestamps --region {AWS_REGION}")
    print("Sync successful" if response == 0 else "Sync failed")

def aws_mv_s3():
    response = os.system(f"aws s3 mv s3://{S3_BUCKET}/binlogs/staging s3://{S3_BUCKET}/binlogs/processed --recursive --metadata-directive REPLACE --metadata '{{\"processed\":\"true\"}}'  --region {AWS_REGION}")
    print("Sync successful" if response == 0 else "Mv failed")
    
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

def write_parquet_append(df):
    df.to_parquet(
        f"s3://{S3_BUCKET}/parquet", 
        partition_cols=["channel", "exchange", "market", "symbol", "date", "hour", "minute"],
        index=False,
        compression="snappy",
        engine="pyarrow"
        )

def clean_up():
    os.system(f"rm -rf binlogs")
    print(f"Deleted: binlogs")

    
def process(channel, levels=10):
    
    paths = sorted((Path(f"/binlogs/{channel}").rglob("*.binlog")))
    
    all_dfs = []
    for path in paths:
        meta = parse_metadata(path)
        snapshots = load_snapshots(path)
        
        if meta["channel"] == "orderbook":
            rows = [normalize_orderbook(s, levels) for s in snapshots]
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
        write_parquet_append(dfs)

if __name__ == "__main__":
    AWS_REGION="us-east-1"
    S3_BUCKET="marketdata001-dev"
    aws_sync_s3()
    aws_mv_s3()
    # process(channel="trades")
    # process(channel="orderbook")
    # clean_up()