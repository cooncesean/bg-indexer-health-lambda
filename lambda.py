import datetime
import json

import boto3
from botocore.vendored import requests


def lambda_handler(event, context):
    """
    Runs through every indexer in BitGo's stack, compares it state to a public
    block explorer, and writes that state to a JSON file on s3 (actually, two
    JSON files, one time-stamped file and another that represents the "latest"
    file that a front-end app will pull from).

    The data structure pushed to s3 will look like:
    """
    current_time = datetime.datetime.now()

    # The number of blocks we allow BitGo to fall behind for a given chain
    # before alerting the dashboard
    BLOCKS_BEHIND_THRESHOLD = 4

    # This dict acts as both a mapping of coin + env to public block explorer as
    # as the final data dict that will be jsonified and persisted to s3 once
    # values like `status`, `latestBlock`, and `blocksBehind` have been populated
    output_data = {
        "metadata": {
            "dateFetched": current_time.strftime('%Y-%m-%d at %H:%M')
        },
        "indexers": {
            "BTC": {
                "name": "Bitcoin",
                "icon": "assets/images/btc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/btc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTC"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST"
                }]
            },
            "LTC": {
                "name": "Litecoin",
                "icon": "assets/images/ltc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/ltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTC"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTCTEST"
                }]
            },
            "BCH": {
                "name": "Bitcoin Cash",
                "icon": "assets/images/bch.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bch/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BCH"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbch/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BCHTEST"
                }]
            },
            "ETH": {
                "name": "Ethereum",
                "icon": "assets/images/eth.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/eth/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ETH"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbch/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ETHTEST"
                }]
            },
            "DASH": {
                "name": "Dash",
                "icon": "assets/images/dash.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/dash/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/DASH"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/DASHTEST"
                }]
            },
            "ZEC": {
                "name": "ZCash",
                "icon": "assets/images/zec.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/zec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZEC"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tzec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZECTEST"
                }]
            },
            "XRP": {
                "name": "Ripple",
                "icon": "assets/images/xrp.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xrp/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/XRP"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txrp/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/XRPTEST"
                }]
            },
            "XLM": {
                "name": "Stellar",
                "icon": "assets/images/xlm.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xlm/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/XLM"
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txlm/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/XRPTEST"
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

            # Hit BitGo's IMS to fetch data about the most recently processed block
            response = requests.get(env_data['bgURL'])
            bg_response = json.loads(response.content)

            # Set default values (assume a healthy status)
            env_data['status'] = True
            env_data['blocksBehind'] = 0
            env_data['latestBlock'] = bg_response['height']

            # Sometimes, BitGo will inform us directly that it's at chainhead.
            # In these cases, assume that that is truthy, and move on with
            # checking against a public block explorer.
            if bg_response.get('chainHead', False):
                # No need to continue processing - keep the default data and move on
                continue

            # Compare the current chain height of BitGo to that of a public
            # block explorer
            response = requests.get(env_data['publicURL'])
            public_response = json.loads(response.content)
            public_block_explorer_height = public_response['data']['blocks']

            # If the difference is greater than our threshold, pitch a fit
            if (public_block_explorer_height - bg_response['height']) > BLOCKS_BEHIND_THRESHOLD:
                env_data['status'] = False
                env_data['blocksBehind'] = '{} blocks'.format(public_block_explorer_height - bg_response['height'])
                env_data['latestBlock'] = bg_response['height']

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
