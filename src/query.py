import json

BASE_ADDRESS = "0x0000000000000000000000000000000000000000"
QUOTE_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

with open("out/uniswap.json", "r") as f:
    pools = json.load(f)

out_pools = {}

for pool_id, pool in pools.items():
    token0 = pool["token0.id"].lower()
    token1 = pool["token1.id"].lower()

    if (
        (token0 == BASE_ADDRESS.lower() and token1 == QUOTE_ADDRESS.lower()) or
        (token1 == BASE_ADDRESS.lower() and token0 == QUOTE_ADDRESS.lower())
    ):
        out_pools[pool_id] = pool

print(f"Found {len(out_pools)} pools")

with open("out/pools.json", "w") as f:
    json.dump(out_pools, f, indent=2)
