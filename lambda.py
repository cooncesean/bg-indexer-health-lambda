MAINNET_KEY_NAME = 'mainnet'
TESTNET_KEY_NAME = 'testnet'
BG_URL = 'bg_url'

import json

from botocore.vendored import requests


COIN_INDEXER_MAPPING = {
    'BTC': {
        MAINNET_KEY_NAME: {
            BG_URL: 'https://www.bitgo.com/api/v2/btc/public/block/latest',
            'handlers': [
                'handlers.sochain.sochainApiHandler',
            ]
        },
        TESTNET_KEY_NAME: {
            BG_URL: 'https://test.bitgo.com/api/v2/tbtc/public/block/latest',
            'handlers': [
                'handlers.sochain.sochainApiHandler',
            ]
        }
    },
}

# Used to populate the status of each indexer the app checks
# Eventually, the datastructure will look like:
# {
#   'BTC': {
#     'mainnet': {
#       'status': True/False,
#       'latest_block': 23421,
#       'blocks_behind': 0,
#     }
#   }
# }
INDEXER_STATUS = {}

for coin, config in COIN_INDEXER_MAPPING.iteritems():
    print coin
    response = requests.get(config[MAINNET_KEY_NAME][BG_URL])
    json_response = json.loads(response.content)

    # Sometimes the IMS response will return the fact that it's at chainhead.
    # If so, trust it (ie: do not compare block height against a public block
    # explorer) and move on
    if res.get('chainHead', False):
        INDEXER_STATUS.setdefault(coin, {})
        INDEXER_STATUS[coin][MAINNET_KEY_NAME]




class BaseHandler(object):
    """
    Handles
    """
