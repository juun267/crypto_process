import requests

api_key = "00c9178f-65f6-4cf7-9fb7-b4433cefdc1d"
limit = 5000

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
headers = {
    "X-CMC_PRO_API_KEY": api_key
}
params = {
    "limit": limit
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

symbol_to_id = {}

# Collect smallest ID for each symbol
for crypto in data["data"]:
    crypto_id = crypto["id"]
    symbol = crypto["symbol"]

    # If symbol is not in dict or current ID is smaller, update
    if symbol not in symbol_to_id or crypto_id < symbol_to_id[symbol]:
        symbol_to_id[symbol] = crypto_id

# Sort by ID
sorted_list = sorted(symbol_to_id.items(), key=lambda x: x[1])

# Print them in ascending order by ID
for symbol, crypto_id in sorted_list:
    print(f"{crypto_id} {symbol}")
