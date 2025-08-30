import requests
import time

acceptable_fee_tiers = {"100", "500", "3000"}
min_liquidity_threshold = 1_000_000
max_tick_spacing = 60


def flatten_pool(pool):
    flat = {
        "feeTier": pool["feeTier"],
        "liquidity": pool["liquidity"],
        "tickSpacing": pool["tickSpacing"],
        "symbol": pool["symbol"],
    }

    for token_key in ["token0", "token1"]:
        for sub_key, value in pool[token_key].items():
            flat[f"{token_key}.{sub_key}"] = value

    return flat

def fetch_uniswap_pools(api_key, subgraph_id, query, batch_size=1000):
    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    flat = {}
    all_pools = []
    skip = 0

    while True:
        variables = {
            "first": batch_size,
            "skip": skip
        }

        response = requests.post(endpoint, json={"query": query, "variables": variables})
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json()
        pools = data.get("data", {}).get("pools", [])

        if not pools:
            break

        all_pools.extend(pools)
        skip += batch_size
        time.sleep(0.2)
        
    for pool in all_pools:
        if pool["hooks"] != "0x0000000000000000000000000000000000000000":
                continue
        if pool["feeTier"] not in acceptable_fee_tiers:
            continue

        if int(pool["tickSpacing"]) > max_tick_spacing:
            continue

        if int(pool["liquidity"]) < min_liquidity_threshold:
            continue
        
        pool["token0"]["decimals"] = int(pool["token0"]["decimals"])
        pool["token1"]["decimals"] = int(pool["token1"]["decimals"])

        flat[pool["id"]] = {
            "feeTier": int(pool["feeTier"]),
            "liquidity": int(pool["liquidity"]),
            "tickSpacing": int(pool["tickSpacing"]),
            "symbol": f"{pool['token0']['symbol']}{pool['token1']['symbol']}",
            **{f"token0.{k}": v for k, v in pool["token0"].items()},
            **{f"token1.{k}": v for k, v in pool["token1"].items()}
        }

    print(len(flat))
    return flat