def normalize_orderbook(snap, levels):
    flat = {"timestamp": snap["timestamp"]}
    for i in range(levels):
        flat[f"bid_price_L{i+1}"] = float(snap["bids"][i][0])
        flat[f"bid_volume_L{i+1}"] = float(snap["bids"][i][1])
        flat[f"ask_price_L{i+1}"] = float(snap["asks"][i][0])
        flat[f"ask_volume_L{i+1}"] = float(snap["asks"][i][1])
    return flat

def normalize_trades(snap):
    return {
        "timestamp": snap["timestamp"],
        "eventTime": snap["eventTime"],
        "tradeId": snap["tradeId"],
        "price": float(snap["price"]),
        "quantity": float(snap["quantity"]),
        "tradeTime": snap["tradeTime"],
        "side": snap["isBuyerMaker"],
    }