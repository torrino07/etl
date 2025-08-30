
import json
import time
import os
from utils.cex import fetch_binance_pairs
from utils.dex import fetch_uniswap_pools
from utils.queries import query
from dotenv import load_dotenv

start_time = time.time()

load_dotenv()

api_key = os.getenv("SUBGRAPH_API_KEY")
subgraph_id = os.getenv("UNISWAP_V4_ETH_SUBGRAPH_ID")

if __name__ == "__main__":
    pools = fetch_uniswap_pools(api_key, subgraph_id, query)
    pairs = fetch_binance_pairs()

    with open("out/uniswap.json", "w") as f:
        json.dump(pools, f, indent=2)

    with open("out/binance.json", "w") as f:
        json.dump(pairs, f, indent=2)
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")
