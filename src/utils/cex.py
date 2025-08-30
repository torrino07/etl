import requests

def fetch_binance_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    result = {}
    
    for symbol_info in data["symbols"]:
        if symbol_info["status"] != "TRADING":
            continue

        filters = {f["filterType"]: f for f in symbol_info["filters"]}
        lot_size = filters.get("LOT_SIZE", {})
        price_filter = filters.get("PRICE_FILTER", {})
        symbol = f"{symbol_info['baseAsset']}{symbol_info['quoteAsset']}"

        result[symbol] = {
            "token0": symbol_info["baseAsset"],
            "token1": symbol_info["quoteAsset"],
            "stepSize": lot_size.get("stepSize"),
            "minQty": lot_size.get("minQty"),
            "tickSize": price_filter.get("tickSize"),
            "symbol": f"{symbol_info['baseAsset']}{symbol_info['quoteAsset']}"
        }
    print(len(result))
    return result
