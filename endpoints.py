import requests
import json

url = 'https://api.viewblock.io/arweave/gateways?network=mainnet'
headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'dnt': '1',
    'if-none-match': 'W/"32dcf-buNM46DWQQLdXODxwFwHpW2VaNU"',
    'origin': 'https://arscan.io',
    'priority': 'u=1, i',
    'referer': 'https://arscan.io/',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}




'''
Gateway response:
{
    "hash": "1H7WZIWhzwTH9FIcnuMqYkTsoyv1OTfGa_amvuYwrgo",
    "statusCode": 2,
    "city": "Toronto",
    "isp": "Cloudflare, Inc.",
    "countryCode": "CA",
    "health": "online",
    "height": 1469599,
    "settings": {
        "port": 443,
        "protocol": "https",
        "minDelegatedStake": 500000000,
        "fqdn": "permagate.io",
        "delegateRewardShareRatio": 100,
        "autoStake": true,
        "note": "Owned and operated by DTF.",
        "allowDelegatedStaking": true,
        "label": "Permagate",
        "properties": "FH1aVetOoulPGqgYukj0VE0wIhDy90WiQoV3U2PeY44"
    },
    "stake": 264510000110
},
'''



def get_gateways():
    response = requests.get(url, headers=headers)
    gateways = response.json().get("docs")
    # filter out offline gateways and only non https protocols
    gateways = [gateway for gateway in gateways if gateway.get("health") == "online" and gateway.get("settings").get("protocol") == "https"]
    # create the gateway url from the protocol and fqdn into a list
    gateways = [f"{gateway.get('settings').get('protocol')}://{gateway.get('settings').get('fqdn')}" for gateway in gateways]
    # dump to file
    json.dump(gateways, open("gateways.json", "w"))
    return gateways


GATEWAYS = get_gateways()
# Print the response (optional)
print(GATEWAYS)