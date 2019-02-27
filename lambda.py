import datetime
import dateutil
import json
import time

import boto3
from botocore.exceptions import ConnectionError
from botocore.vendored import requests


class PublicBlockExplorerHandler:
    """
    This class based function handles the calling and parsing of urls that
    point at public block explorers. The function always returns the height
    of the current public block on the network specified.

    We compare this height to the height of our internal block explorers to
    ensure that we are at or near chainhead.
    """
    def get_url_and_return_height(self, public_block_explorer_url):
        """
        Takes the URL of the public block explore and returns the height of the
        most current public block after requesting the url and parsing the repsonse.
        """
        response = requests.get(public_block_explorer_url)
        print(public_block_explorer_url)
        print(response)
        retry_count = 0
        status_code = response.status_code
        while status_code != 200:
            time.sleep(8)
            if retry_count > 4:
                break
            response = requests.get(public_block_explorer_url)
            status_code = response.status_code
            if status_code >= 500:
                break

        try:
            public_response = json.loads(response.content)
        except:
            public_response = {}

        # Parse the response and return the public height of the blockchain.
        # This is where subclasses typically override behavior to handle custom
        # response parsing based on the public explorer being called.
        try:
            public_block_explorer_height = self.parse_request_and_return_height(public_response)
        except KeyError:
            public_block_explorer_height = 0

        return public_block_explorer_height

    def parse_request_and_return_height(self, public_response):
        """
        Hook that allows subclassess to implement custom response parsing logic.
        """
        raise NotImplementedError


class ChainSoAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for Chain.so (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BTC, LTC, DASH, ZEC

    Sample URL: https://chain.so/api/v2/get_info/BTC
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['data']['blocks']


class InsightAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for Insight (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TDASH

    Sample URL: https://test.insight.dash.siampm.com/api/blocks
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['blocks'][0]['height']


class BTCDotComAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for btc.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BCH

    Sample URL: https://bch-chain.api.btc.com/v3/block/latest/
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['data']['height']

class BitcoinDotComAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for bitcoin.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TBCH

    Sample URL: https://explorer.bitcoin.com/api/tbch/blocks/?limit=1
    """
    def parse_request_and_return_height(self, public_response):
        return int(public_response['blocks'][0]['height'])


class EtherscanAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for etherscan.io (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: ETH, TETH (Kovan)

    Sample URL: https://kovan.etherscan.io/api?module=proxy&action=eth_blockNumber
    """
    def parse_request_and_return_height(self, public_response):
        return int(public_response['result'], 16)


class RippleAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for data.ripple.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XRP

    Sample URL: https://data.ripple.com/v2/ledgers/
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['ledger']['ledger_index']


class StellarAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for stellar.org (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XLM, TXLM

    Sample URL: https://horizon.stellar.org/ledgers?order=desc
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['_embedded']['records'][0]['sequence']


class BlockchairAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for blockchair (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: BSV

    Sample URL: https://api.blockchair.com/bitcoin-sv/blocks?limit=1
    """
    def parse_request_and_return_height(self, public_response):
        return public_response['data'][0]['id']


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
                "icon": "assets/images/btc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/btc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTC",
                    "apiHandler": ChainSoAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": ChainSoAPIHandler
                }]
            },
            "BTC": {
                "name": "v2 Bitcoin",
                "icon": "assets/images/btc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/btc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTC",
                    "apiHandler": ChainSoAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/BTCTEST",
                    "apiHandler": ChainSoAPIHandler
                }]
            },
            "LTC": {
                "name": "Litecoin",
                "icon": "assets/images/ltc.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/ltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTC",
                    "apiHandler": ChainSoAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/LTCTEST",
                    "apiHandler": ChainSoAPIHandler
                }]
            },
            "BCH": {
                "name": "Bitcoin Cash",
                "icon": "assets/images/bch.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bch/public/block/latest",
                    "publicURL": "https://bch-chain.api.btc.com/v3/block/latest/",
                    "apiHandler": BTCDotComAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbch/public/block/latest",
                    "publicURL": "https://explorer.bitcoin.com/api/tbch/blocks/?limit=1",
                    "apiHandler": BitcoinDotComAPIHandler,
                }]
            },
            "BSV ": {
                "name": "Bitcoin SV",
                "icon": "assets/images/bsv.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bsv/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin-sv/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler,
                },
                {
                    # no public testnet block explorer
                    "network": "TestNet",
                }]
            },
            "ETH": {
                "name": "Ethereum",
                "icon": "assets/images/eth.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/eth/public/block/latest",
                    "publicURL": "https://api.etherscan.io/api?module=proxy&action=eth_blockNumber",
                    "apiHandler": EtherscanAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/teth/public/block/latest",
                    "publicURL": "https://kovan.etherscan.io/api?module=proxy&action=eth_blockNumber",
                    "apiHandler": EtherscanAPIHandler,
                }]
            },
            "DASH": {
                "name": "Dash",
                "icon": "assets/images/dash.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/dash/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/DASH",
                    "apiHandler": ChainSoAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://test.insight.dash.siampm.com/api/blocks",
                    "apiHandler": InsightAPIHandler,
                }]
            },
            "ZEC": {
                "name": "ZCash",
                "icon": "assets/images/zec.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/zec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZEC",
                    "apiHandler": ChainSoAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tzec/public/block/latest",
                    "publicURL": "https://chain.so/api/v2/get_info/ZECTEST",
                    "apiHandler": ChainSoAPIHandler
                }]
            },
            "XRP": {
                "name": "Ripple",
                "icon": "assets/images/xrp.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xrp/public/block/latest",
                    "publicURL": "https://data.ripple.com/v2/ledgers/",
                    "apiHandler": RippleAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txrp/public/block/latest",
                    "publicURL": "https://testnet.data.api.ripple.com/v2/ledgers",
                    "apiHandler": RippleAPIHandler,
                }]
            },
            "XLM": {
                "name": "Stellar",
                "icon": "assets/images/xlm.png",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xlm/public/block/latest",
                    "publicURL": "https://horizon.stellar.org/ledgers?order=desc",
                    "apiHandler": StellarAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txlm/public/block/latest",
                    "publicURL": "https://horizon-testnet.stellar.org/ledgers?order=desc",
                    "apiHandler": StellarAPIHandler,
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
                env_data['status'] = True
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

            bg_response = json.loads(response.content)

            # If `height` isn't in the response, raise a red flag; this happens
            # when the IMS has gotten into a wierd state.
            if 'height' not in bg_response:
                env_data['status'] = False
                env_data['latestBlock'] = 'IMS Unresponsive'
                env_data['blocksBehind'] = 'IMS Unresponsive'
                continue

            # Use the handler defined on the coin + network to parse the response
            # and return the public height of the blockchain.
            # Pop it from the dict at the same time; we don't want to provide it
            # in the serialized JSON output.
            api_handler_class = env_data.pop('apiHandler')
            api_handler = api_handler_class()
            try:
                public_block_explorer_height = api_handler.get_url_and_return_height(env_data['publicURL'])
            except KeyError:
                public_block_explorer_height = 0

            # Set values (assume a healthy status; it is flipped below if the
            # chain head delta exceeds our threshold)
            env_data['status'] = True
            env_data['latestBlock'] = bg_response['height']
            env_data['blocksBehind'] = '{} blocks'.format(public_block_explorer_height - bg_response['height'])

            # If the difference is greater than our threshold, pitch a fit
            if (public_block_explorer_height - bg_response['height']) > BLOCKS_BEHIND_THRESHOLD:
                env_data['status'] = False

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
