import datetime
import dateutil
import json
import time

import boto3
from botocore.exceptions import ConnectionError
from botocore.vendored import requests


def algorand_api_handler(response_data):
    """
    An API handler for algoexplorer.io (a public block explorer) API responses. Returns
    the block height (round) for the given coin + network.

    Used to parse: ALGO

    Sample URL: https://api.algoexplorer.io/v1/block/latest/1
    """
    return response_data[0]['round']


def chainso_api_handler(response_data):
    """
    An API handler for Chain.so (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BTC, LTC, DASH, ZEC

    Sample URL: https://chain.so/api/v2/get_info/BTC
    """
    return response_data['data']['blocks']


def insight_api_handler(response_data):
    """
    An API handler for insight (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TDASH

    Sample URL: https://testnet-insight.dashevo.org/insight-api/blocks
    """
    return response_data['blocks'][0]['height']


def btcdotcom_api_handler(response_data):
    """
    An API handler for btc.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BCH

    Sample URL: https://bch-chain.api.btc.com/v3/block/latest/
    """
    return response_data['data']['height']


def bitcoin_dot_com_api_handler(response_data):
    """
    An API handler for bitcoin.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TBCH

    Sample URL: https://explorer.bitcoin.com/api/tbch/blocks/?limit=1
    """
    return int(response_data['blocks'][0]['height'])

def etherchain_api_handler(response_data):
    """
    An API handler for etherchain.org (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: ETH

    Sample URL: https://www.etherchain.org/blocks/data
    """
    return response_data['recordsTotal']


def blockscout_api_handler(response_data):
    """
    An API handler for blockscout.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TETH (Kovan)

    Sample URL: https://blockscout.com/eth/kovan/blocks?type=JSON
    """
    return int(response_data['items'][0].split('data-block-number="')[-1].split('"')[0])


def ripple_api_handler(response_data):
    """
    An API handler for data.ripple.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XRP

    Sample URL: https://data.ripple.com/v2/ledgers/
    """
    return response_data['ledger']['ledger_index']


def stellar_api_handler(response_data):
    """
    An API handler for data.ripple.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XRP

    Sample URL: https://horizon.stellar.org/ledgers?order=desc
    """
    return response_data['_embedded']['records'][0]['sequence']


def blockchair_api_handler(response_data):
    """
    An API handler for blockchair (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BSV

    Sample URL: https://api.blockchair.com/bitcoin-sv/blocks?limit=1
    """
    return response_data['data'][0]['id']

def lambda_handler(event, context):
    """
    Runs through every indexer in BitGo's stack, compares it state to a public
    block explorer, and writes that state to a JSON file on s3 (actually, two
    JSON files, one time-stamped file and another that represents the "latest"
    file that a front-end app will pull from).

    The final data structure pushed to s3 looks like: https://s3-us-west-2.amazonaws.com/bitgo-indexer-health/latest.json
    """
    pst = dateutil.tz.gettz('US/Pacific')
    current_time = datetime.datetime.now(tz=pst)

    # The number of blocks we allow BitGo to fall behind for a given chain
    # before alerting the dashboard
    BLOCKS_BEHIND_THRESHOLD = 4

    # This dict acts as both a mapping of coin + env to public block explorer as
    # as the final data dict that will be jsonified and persisted to s3 once
    # values like `status`, `latestBlock`, and `blocksBehind` have been populated
    output_data = {
        "metadata": {
            "dateFetched": current_time.strftime('%Y-%m-%d at %I:%M%p PST')
        },
        "indexers": {
            "v1BTC": {
                "name": "v1 Bitcoin",
                "icon": "assets/images/btc_v1.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTC",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": chainso_api_handler
                }]
            },
            "BTC": {
                "name": "v2 Bitcoin",
                "icon": "assets/images/btc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/btc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTC",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": chainso_api_handler
                }]
            },
            "LTC": {
                "name": "Litecoin",
                "icon": "assets/images/ltc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/ltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTC",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTCTEST",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTCTEST",
                    "apiHandler": chainso_api_handler
                }]
            },
            "BCH": {
                "name": "Bitcoin Cash",
                "icon": "assets/images/bch.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bch/public/block/latest",
                    "publicURL": "https://bch-chain.api.btc.com/v3/block/latest/",
                    "apiHandler": btcdotcom_api_handler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbch/public/block/latest",
                    "publicURL": "https://explorer.bitcoin.com/api/tbch/blocks/?limit=1",
                    "apiHandler": bitcoin_dot_com_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tbch/public/block/latest",
                    "publicURL": "https://explorer.bitcoin.com/api/tbch/blocks/?limit=1",
                    "apiHandler": bitcoin_dot_com_api_handler,
                }]
            },
            "ETH": {
                "name": "Ethereum",
                "icon": "assets/images/eth.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/eth/public/block/latest",
                    "publicURL": "https://www.etherchain.org/blocks/data",
                    "apiHandler": etherchain_api_handler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/teth/public/block/latest",
                    "publicURL": "https://blockscout.com/eth/kovan/blocks?type=JSON",
                    "apiHandler": blockscout_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/teth/public/block/latest",
                    "publicURL": "https://blockscout.com/eth/kovan/blocks?type=JSON",
                    "apiHandler": blockscout_api_handler,
                }]
            },
            "DASH": {
                "name": "Dash",
                "icon": "assets/images/dash.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/dash/public/block/latest",
                    "publicURL": "https://api.blockchair.com/dash/blocks?limit=1",
                    "apiHandler": blockchair_api_handler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://testnet-insight.dashevo.org/insight-api/blocks",
                    "apiHandler": insight_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://testnet-insight.dashevo.org/insight-api/blocks",
                    "apiHandler": insight_api_handler,
                }]
            },
            "ZEC": {
                "name": "ZCash",
                "icon": "assets/images/zec.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/zec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZEC",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tzec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZECTEST",
                    "apiHandler": chainso_api_handler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tzec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZECTEST",
                    "apiHandler": chainso_api_handler
                }]
            },
            "XRP": {
                "name": "Ripple",
                "icon": "assets/images/xrp.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xrp/public/block/latest",
                    "publicURL": "https://data.ripple.com/v2/ledgers/",
                    "apiHandler": ripple_api_handler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txrp/public/block/latest",
                    "publicURL": "https://testnet.data.api.ripple.com/v2/ledgers",
                    "apiHandler": ripple_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/txrp/public/block/latest",
                    "publicURL": "https://testnet.data.api.ripple.com/v2/ledgers",
                    "apiHandler": ripple_api_handler,
                }]
            },
            "XLM": {
                "name": "Stellar",
                "icon": "assets/images/xlm.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xlm/public/block/latest",
                    "publicURL": "https://horizon.stellar.org/ledgers?order=desc",
                    "apiHandler": stellar_api_handler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txlm/public/block/latest",
                    "publicURL": "https://horizon-testnet.stellar.org/ledgers?order=desc",
                    "apiHandler": stellar_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/txlm/public/block/latest",
                    "publicURL": "https://horizon-testnet.stellar.org/ledgers?order=desc",
                    "apiHandler": stellar_api_handler,
                }]
            },
            "BSV ": {
                "name": "Bitcoin SV",
                "icon": "assets/images/bsv.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bsv/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin-sv/blocks?limit=1",
                    "apiHandler": blockchair_api_handler,
                },
                {
                    # no public testnet block explorer
                    "network": "TestNet",
                    "apiHandler": blockchair_api_handler,
                },
                {
                    # no public testnet block explorer
                    "network": "Dev",
                    "apiHandler": blockchair_api_handler,
                }]
            },
            "ALGO": {
                "name": "Algorand",
                "icon": "assets/images/algo.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/algo/public/block/latest",
                    "publicURL": "https://api.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": algorand_api_handler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/talgo/public/block/latest",
                    "publicURL": "https://api.testnet.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": algorand_api_handler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/talgo/public/block/latest",
                    "publicURL": "https://api.testnet.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": algorand_api_handler,
                }]
            },
        }
    }

    # Iterate over the dict above and fill in the blanks:
    # 1. Hit BitGo to get the state of the indexer
    # 2. Hit a public block explorer to compare chainheads
    # 3. Add the state to the dict
    for coin_symbol, coin_data in output_data['indexers'].items():
        for env_data in coin_data['environments']:

            # If a bgURL is not defined for a particular env, return
            if 'bgURL' not in env_data:
                env_data['status'] = False
                env_data['latestBlock'] = 'No Public URL'
                env_data['blocksBehind'] = 'n/a'
                continue

            # Hit BitGo's IMS to fetch data about the most recently processed block
            try:
                response = requests.get(env_data['bgURL'], timeout=4)
            # If the server took more than four seconds to respond, consider it
            # down and alert the status
            except ConnectionError:
                env_data['status'] = False
                env_data['latestBlock'] = 'IMS Unresponsive'
                env_data['blocksBehind'] = 'IMS Unresponsive'
                continue

            print(env_data['bgURL'])
            print(env_data['publicURL'])
            try:
                bg_response = json.loads(response.content)
            except json.JSONDecodeError:
                env_data['status'] = False
                env_data['latestBlock'] = 'IMS Unresponsive'
                env_data['blocksBehind'] = 'IMS Unresponsive'
                continue

            # If `height` isn't in the response, raise a red flag; this happens
            # when the IMS has gotten into a wierd state.
            if 'height' not in bg_response:
                env_data['status'] = False
                env_data['latestBlock'] = 'IMS Unresponsive'
                env_data['blocksBehind'] = 'IMS Unresponsive'
                continue

            # In all cases, we use the same URL to fetch public Dev and Test
            # block explorer data. Instead of making another round-trip to the
            # service, use the cached response from the TestNet call
            #
            # (this is kinda nasty; this massive conditional stinks and i'm not
            # a fan of the brittleness introduced by assuming the list of envs
            # for each coin will be 'prod', 'test', 'dev'.... but, side project
            # ¯\_(ツ)_/¯ )
            if env_data['network'] == 'Dev':
                env_data.pop('apiHandler')  # remove this fcn as it is not json serializable and should not be included in the json output
                public_block_explorer_height = coin_data['environments'][1].get('referenceBlock', 0)
            else:
                response = requests.get(env_data['publicURL'], verify=False)
                retry_count = 0
                status_code = response.status_code
                while status_code != 200:
                    time.sleep(8)
                    if retry_count > 4:
                        break
                    response = requests.get(env_data['publicURL'])
                    status_code = response.status_code
                    if status_code >= 500:
                        break

                try:
                    public_response = json.loads(response.content)
                except:
                    public_response = {}

                # Use the handler defined on the coin + network to parse the response
                # and return the public height of the blockchain.
                # Pop it from the dict at the same time; we don't want to provide it
                # in the serialized JSON output.
                api_handler = env_data.pop('apiHandler')
                try:
                    public_block_explorer_height = api_handler(public_response)
                except KeyError:
                    public_block_explorer_height = 0

            # Set values (assume a healthy status; it is flipped below if the
            # chain head delta exceeds our threshold)
            env_data['status'] = True
            env_data['latestBlock'] = bg_response['height']
            env_data['referenceBlock'] = public_block_explorer_height
            env_data['blocksBehind'] = '{} blocks'.format(int(public_block_explorer_height) - int(bg_response['height']))

            # If the difference is greater than our threshold, pitch a fit
            if (int(public_block_explorer_height) - int(bg_response['height'])) > BLOCKS_BEHIND_THRESHOLD:
                env_data['status'] = False

    # Iterate through the dict and pop any non-json serializable objects that
    # are about to be json dump'd
    for k, v in output_data['indexers'].items():
        for env_data in v['environments']:
            if 'apiHandler' in env_data:
                env_data.pop('apiHandler')

    # Jsonify the output dict
    string = json.dumps(output_data)
    encoded_string = string.encode("utf-8")

    # Create the file names; we create two:
    # 1. A timestamped version of the json for historical purposes
    # 2. A file called 'latest' which overwrites the previously marked 'latest'
    # file. This is the file that the front-end app consumes. Overwriting it every
    # five minutes keeps the app up to date.
    dated_file_name = "{}.json".format(current_time)
    latest_file_name = "latest.json"

    # Write the files to the bucket
    bucket_name = "bitgo-indexer-health"
    s3 = boto3.resource("s3")
    aws_kwargs = {
        'Body': encoded_string,
        'ACL': 'public-read',
        'ContentType': 'application/json',
    }
    s3.Bucket(bucket_name).put_object(Key=dated_file_name, **aws_kwargs)
    s3.Bucket(bucket_name).put_object(Key=latest_file_name, **aws_kwargs)
