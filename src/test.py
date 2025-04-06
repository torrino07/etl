import os, json, struct, pandas, s3fs, numpy
from pathlib import Path
from utils.DataParser import normalize_orderbook, normalize_trades

S3_INPUT_ROOT = "s3://marketdata001-dev/binlogs"
S3_OUTPUT_ROOT = "s3://marketdata001-dev/parquet"
LEVELS = 10

s3 = s3fs.S3FileSystem(anon=False)

def load_snapshots(s3_path):
    snapshots = []
    with s3.open(s3_path, "rb") as f:
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

def parse_metadata(s3_path):
    parts = Path(s3_path).parts
    return {
        "channel": parts[2],
        "exchange": parts[3],
        "market": parts[4],
        "symbol": parts[5],
        "date": parts[6],
        "hour": parts[7],
        "minute": parts[8],
    }
    
def build_dataframe(s3_file_path):
    meta = parse_metadata(s3_file_path)
    raw = load_snapshots(s3_file_path)
    if meta["channel"] == "orderbook":
        rows = [normalize_orderbook(s, levels=LEVELS) for s in raw]
        df = pandas.DataFrame(rows)
        df["timestamp"] = pandas.to_datetime(df["timestamp"])
    elif meta["channel"] == "trades":
        rows = [normalize_trades(s) for s in raw]
        df = pandas.DataFrame(rows)
        df["timestamp"] = pandas.to_datetime(df["tradeTime"], unit="ms")
    else:
        raise ValueError(f"Unknown channel: {meta['channel']}")
    for k, v in meta.items():
        df[k] = v
    return df

def write_parquet_append(df, output_root):
    df.to_parquet(
        output_root, 
        partition_cols=["channel", "exchange", "market", "symbol", "date", "hour", "minute"],
        index=False,
        compression="snappy",
        engine="pyarrow"
        )
    print(f"Appended file: {output_root}")
    
def run(s3_input_root, s3_output_root, channel):
    all_dfs = []
    all_objects = s3.glob(s3_input_root + f"/{channel}" + "/**")
    done_dirs = {str(Path(obj).parent) for obj in all_objects if obj.endswith(".done")}
    binlogs_paths = [obj for obj in all_objects if obj.endswith(".binlog") and str(Path(obj).parent) in done_dirs]
    for path in binlogs_paths:
        df = build_dataframe(path)
        if df is not None:
            all_dfs.append(df)
    
    if all_dfs:
        dfs = pandas.concat(all_dfs, ignore_index=True)
        write_parquet_append(dfs, s3_output_root)

run(s3_input_root=S3_INPUT_ROOT, s3_output_root=S3_OUTPUT_ROOT, channel="orderbook")
run(s3_input_root=S3_INPUT_ROOT, s3_output_root=S3_OUTPUT_ROOT, channel="trades")