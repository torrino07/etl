query = """
query PoolsPage($first: Int!, $skip: Int!) {
  pools(first: $first, skip: $skip, orderBy: id, orderDirection: asc) {
    id
    feeTier
    tickSpacing
    hooks
    liquidity
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
"""