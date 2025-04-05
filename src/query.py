import pyarrow.dataset as ds
import pyarrow.parquet as pq

path = "output_parquet/channel=orderbook/exchange=binance/market=spot/symbol=BTCUSDT/date=2025-04-04/hour=21/BTCUSDT-2025-04-04-21.parquet"
schema = pq.read_schema(path)

channel = "trades"
dataset = ds.dataset(f"output_parquet/channel={channel}", format="parquet", partitioning="hive")

table = dataset.to_table(
    filter=(
        (ds.field("exchange") == "binance") &
        (ds.field("market") == "spot") &
        (ds.field("symbol") == "BTCUSDT") &
        (ds.field("date") == "2025-04-04") &
        (ds.field("hour") == 21)
    )
)

df = table.to_pandas()
print(df)
