import json, struct, pyarrow
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
from utils.DataParser import normalize_orderbook, normalize_trades
    
def parse_metadata(path):
    parts = Path(path).parts
    return {
        "channel": parts[6],
        "exchange": parts[7],
        "market": parts[8],
        "symbol": parts[9],
        "date": parts[10],
        "hour": parts[11],
        "minute": parts[12],
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

def write_partitioned(table):
    pq.write_to_dataset(
        table,
        root_path=f"s3://{S3_BUCKET}/parquet",
        partition_cols=["channel", "exchange", "market", "symbol", "date", "hour", "minute"],
        compression="snappy"
        )

def main():
    
    format = "%Y-%m-%d/%H/%M/"
    current_date_time_obj = datetime.now() - timedelta(hours=2)
    current_date_time = current_date_time_obj.strftime(format)
    next_date_time = (current_date_time_obj + timedelta(minutes=1)).strftime(format)
    paths = sorted(list(map(str, Path(PATH).rglob("*.binlog"))))
  
    trades, orderbook = [], []
    for path in paths:
        if current_date_time not in path and next_date_time not in path:
            meta = parse_metadata(path)
            snapshots = load_snapshots(path)
            channel = meta["channel"]
            
            for snap in snapshots:
                if channel == "orderbook":
                    rec = normalize_orderbook(snap, levels=10)
                    orderbook.append({**rec, **meta})
                elif channel == "trades":
                    rec = normalize_trades(snap)
                    trades.append({**rec, **meta})
                else:
                    raise ValueError(f"Unknown channel: {channel}")
                
            Path(path).unlink(missing_ok=True)
            
    write_partitioned(pyarrow.Table.from_pylist(trades))
    write_partitioned(pyarrow.Table.from_pylist(orderbook))

if __name__ == "__main__":
    AWS_REGION="us-east-1"
    S3_BUCKET="marketdata001-dev"
    PATH="/Users/dorian/mnt/shared_data/binlogs"
    main()
    