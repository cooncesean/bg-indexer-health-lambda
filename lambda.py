import datetime
import dateutil
import json
import re
import time

import boto3
from botocore.vendored.requests.adapters import ReadTimeout
from botocore.exceptions import ConnectionError
import jsonrpcclient
import requests


class PublicBlockExplorerHandler:
    """
    This class based function handles the calling and parsing of urls that
    point at public block explorers. The function always returns the height
    of the current public block on the specified network.

    We compare this height to the height of our internal block explorers to
    ensure that we are at or near chainhead.
    """
    def get_url_and_return_height(self, public_block_explorer_url):
        """
        Takes the URL of the public block explore and returns the height of the
        most current public block after requesting the url and parsing the repsonse.

        This method may also be overridden in a subclass should a public block
        explorer specified is not available via an http + JSON request/response
        cycle.
        """
        print(public_block_explorer_url)
        response = requests.get(public_block_explorer_url, timeout=5)
        print(response.status_code)
        retry_count = 0
        status_code = response.status_code
        while status_code != 200:
            time.sleep(8)
            if retry_count > 4:
                break
            response = requests.get(public_block_explorer_url, timeout=5)
            print('Retried {} times...'.format(retry_count))
            print(response.status_code)
            status_code = response.status_code
            retry_count += 1
            if status_code >= 500:
                break

        # Parse the response and return the public height of the blockchain.
        # This is where subclasses typically override behavior to handle custom
        # response parsing based on the public explorer being called.
        try:
            public_block_explorer_height = self.parse_request_and_return_height(response)
        except KeyError:
            public_block_explorer_height = 0

        return public_block_explorer_height

    def parse_request_and_return_height(self, response):
        """
        Hook that allows subclassess to implement custom response parsing logic.
        """
        raise NotImplementedError


class BlockchairAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for blockchair (a public block explorer) API responses.

    Used to parse: BSV, BCH, and Litecoin mainnet explorers

    Sample URL: https://api.blockchair.com/bitcoin-sv/blocks?limit=1
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['data'][0]['id']


class CryptoidAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for Cryptoid (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: DASH mainnet

    Sample URL: https://chainz.cryptoid.info/explorer/index.data.dws?coin=dash&n=1
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['blocks'][0]['height']


class InsightAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for Insight (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TDASH

    Sample URL: https://test.insight.dash.siampm.com/api/blocks
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['blocks'][0]['height']


class ImginaryDotCashAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for http://testnet.imaginary.cash (a public block explorer)
    API responses. Returns the block height for the given coin + network.

    Used to parse: TBCH

    Sample URL: http://testnet.imaginary.cash/blocks
    """
    def parse_request_and_return_height(self, response):
        """
        This is NOT a json response; we need to parse HTML to find the most
        recent block.
        """
        # Parse the response to find the first (most recent) block in the table
        page_content = response.content
        if not isinstance(page_content, str):
            page_content = str(page_content)

        table_cell_text = re.search(r'<td class="data-cell monospace">(.*?)</td>', page_content).group(1)
        return table_cell_text.split('"')[1].split('/')[2]


class LitecoinToolsAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for litecointools (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TLTC

    Sample URL: http://testnet.litecointools.com/status
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['info']['blocks']


class ZchaApiHandler(PublicBlockExplorerHandler):
    """
    An API handler for zchain (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: ZEC

    Sample URL: https://api.zcha.in/v2/mainnet/network
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['blockNumber']


class EtherscanAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for etherscan.io (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: ETH, TETH (Kovan)

    Sample URL: https://kovan.etherscan.io/api?module=proxy&action=eth_blockNumber
    """
    def parse_request_and_return_height(self, response):

        json_response = json.loads(response.content)
        return int(json_response['result'], 16)


class RippleAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for data.ripple.com (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XRP

    Sample URL: https://data.ripple.com/v2/ledgers/
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['ledger']['ledger_index']


class AltNetTestnetRippleAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for s.altnet.rippletest.net API responses. Returns
    the block height for the given coin + network.

    Used to parse: TXRP

    Sample URL: https://s.altnet.rippletest.net:51234
    """
    def get_url_and_return_height(self, public_block_explorer_url):
        """
        This ripple testnset explorer endpoint uses jrpc.
        """
        try:
            resp = jsonrpcclient.request(public_block_explorer_url, 'ledger_current')
        except Exception as e:
            return re.search(r'ledger_current_index\': (.*?)}}', e.message).group(1)
        return None

    def parse_request_and_return_height(self, response):
        """
        The value is already parsed in get_url_and_return_height() - simply return
        this value.
        """
        return response


class AlgoExplorerAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for s.altnet.rippletest.net API responses. Returns
    the block height for the given coin + network.

    Used to parse: ALGO, TALGO

    Sample URL: https://api.testnet.algoexplorer.io/v1/block/latest/1
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response[0]['round']


class EOSAPIHander(PublicBlockExplorerHandler):
    """
    An API handler for s.altnet.rippletest.net API responses. Returns
    the block height for the given coin + network.

    Used to parse: EOS, TEOS

    Sample URL: https://api.eosnewyork.io/v1/chain/get_info and
    http://jungle2.cryptolions.io/v1/chain/get_info (both return similar responses).
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['head_block_num']


class StellarAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for stellar.org (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: XLM, TXLM

    Sample URL: https://horizon.stellar.org/ledgers?order=desc
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['_embedded']['records'][0]['sequence']


class TronAPIHandler(PublicBlockExplorerHandler):
    """
    An API handler for stellar.org (a public block explorer) API responses. Returns
    the block height for the given coin + network.

    Used to parse: TRX, TTRX

    Sample URL: https://api.shasta.tronscan.org/api/system/status
    """
    def parse_request_and_return_height(self, response):
        json_response = json.loads(response.content)
        return json_response['database']['block']


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
                "icon": "https://app.bitgo.com/assets/img/icons/BTC.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/testnet/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v1/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/testnet/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                }]
            },
            "BTC": {
                "name": "v2 Bitcoin",
                "icon": "https://app.bitgo.com/assets/img/icons/BTC.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/btc/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/testnet/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tbtc/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin/testnet/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                }]
            },
            "LTC": {
                "name": "Litecoin",
                "icon": "https://app.bitgo.com/assets/img/icons/LTC.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/ltc/public/block/latest",
                    "publicURL": "https://api.blockchair.com/litecoin/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "http://testnet.litecointools.com/status",
                    "apiHandler": LitecoinToolsAPIHandler
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tltc/public/block/latest",
                    "publicURL": "http://testnet.litecointools.com/status",
                    "apiHandler": LitecoinToolsAPIHandler
                }]
            },
            "BCH": {
                "name": "Bitcoin Cash",
                "icon": "https://app.bitgo.com/assets/img/icons/BCH.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bch/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin-cash/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler,
                },
                {
                    "network": "TestNet",
                    # "bgURL": "https://test.bitgo.com/api/v2/tbch/public/block/latest",
                    # "publicURL": "http://testnet.imaginary.cash/blocks",
                    # "apiHandler": ImginaryDotCashAPIHandler,
                },
                {
                    "network": "Dev",
                    # "bgURL": "https://webdev.bitgo.com/api/v2/tbch/public/block/latest",
                    # "publicURL": "http://testnet.imaginary.cash/blocks",
                    # "apiHandler": ImginaryDotCashAPIHandler,
                }]
            },
            "BSV ": {
                "name": "Bitcoin SV",
                "icon": "https://app.bitgo.com/assets/img/icons/BSV.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/bsv/public/block/latest",
                    "publicURL": "https://api.blockchair.com/bitcoin-sv/blocks?limit=1",
                    "apiHandler": BlockchairAPIHandler,
                },
                {
                    # no public testnet block explorer
                    "network": "TestNet",
                    "apiHandler": None,
                },
                {
                    # no public testnet block explorer
                    "network": "Dev",
                    "apiHandler": None,
                }]
            },
            "ETH": {
                "name": "Ethereum",
                "icon": "https://app.bitgo.com/assets/img/icons/ETH.svg",
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
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/teth/public/block/latest",
                    "publicURL": "https://kovan.etherscan.io/api?module=proxy&action=eth_blockNumber",
                    "apiHandler": EtherscanAPIHandler,
                }]
            },
            "DASH": {
                "name": "Dash",
                "icon": "https://app.bitgo.com/assets/img/icons/DASH.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/dash/public/block/latest",
                    "publicURL": "https://chainz.cryptoid.info/explorer/index.data.dws?coin=dash&n=1",
                    "apiHandler": CryptoidAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://testnet-insight.dashevo.org/insight-api/blocks",
                    "apiHandler": InsightAPIHandler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/tdash/public/block/latest",
                    "publicURL": "https://testnet-insight.dashevo.org/insight-api/blocks",
                    "apiHandler": InsightAPIHandler,
                }]
            },
            "ZEC": {
                "name": "ZCash",
                "icon": "https://app.bitgo.com/assets/img/icons/ZEC.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/zec/public/block/latest",
                    "publicURL": "https://api.zcha.in/v2/mainnet/network",
                    "apiHandler": ZchaApiHandler
                },
                {
                    # no public testnet block explorer
                    "network": "TestNet",
                    "apiHandler": None,
                },
                {
                    # no public testnet block explorer
                    "network": "Dev",
                    "apiHandler": None,
                }]
            },
            "XRP": {
                "name": "Ripple",
                "icon": "https://app.bitgo.com/assets/img/icons/XRP.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/xrp/public/block/latest",
                    "publicURL": "https://data.ripple.com/v2/ledgers/",
                    "apiHandler": RippleAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/txrp/public/block/latest",
                    # "publicURL": "https://s.altnet.rippletest.net:51234",
                    # "apiHandler": AltNetTestnetRippleAPIHandler,
                    "apiHandler": None,

                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/txrp/public/block/latest",
                    # "publicURL": "https://s.altnet.rippletest.net:51234",
                    # "apiHandler": AltNetTestnetRippleAPIHandler,
                    "apiHandler": None,
                }]
            },
            "XLM": {
                "name": "Stellar",
                "icon": "https://app.bitgo.com/assets/img/icons/XLM.svg",
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
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/txlm/public/block/latest",
                    "publicURL": "https://horizon-testnet.stellar.org/ledgers?order=desc",
                    "apiHandler": StellarAPIHandler,
                }]
            },
            "ALGO": {
                "name": "Algorand",
                "icon": "https://app.bitgo.com/assets/img/icons/ALGO.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/algo/public/block/latest",
                    "publicURL": "https://api.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": AlgoExplorerAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/talgo/public/block/latest",
                    "publicURL": "https://api.testnet.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": AlgoExplorerAPIHandler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/talgo/public/block/latest",
                    "publicURL": "https://api.testnet.algoexplorer.io/v1/block/latest/1",
                    "apiHandler": AlgoExplorerAPIHandler,
                }]
            },
            "EOS": {
                "name": "EOS",
                "icon": "https://app.bitgo.com/assets/img/icons/EOS.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/eos/public/block/latest",
                    "publicURL": "https://bp.cryptolions.io/v1/chain/get_info",
                    "apiHandler": EOSAPIHander,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/teos/public/block/latest",
                    "publicURL": "http://jungle2.cryptolions.io/v1/chain/get_info",
                    "apiHandler": EOSAPIHander,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/teos/public/block/latest",
                    "publicURL": "http://jungle2.cryptolions.io/v1/chain/get_info",
                    "apiHandler": EOSAPIHander,
                }]
            },
            "TRX": {
                "name": "TRX",
                "icon": "https://app.bitgo.com/assets/img/icons/TRX.svg",
                "environments": [{
                    "network": "MainNet",
                    "bgURL": "https://www.bitgo.com/api/v2/trx/public/block/latest",
                    "publicURL": "https://apilist.tronscan.org/api/system/status",
                    "apiHandler": TronAPIHandler,
                },
                {
                    "network": "TestNet",
                    "bgURL": "https://test.bitgo.com/api/v2/ttrx/public/block/latest",
                    "publicURL": "https://api.shasta.tronscan.org/api/system/status",
                    "apiHandler": TronAPIHandler,
                },
                {
                    "network": "Dev",
                    "bgURL": "https://webdev.bitgo.com/api/v2/ttrx/public/block/latest",
                    "publicURL": "https://api.shasta.tronscan.org/api/system/status",
                    "apiHandler": TronAPIHandler,
                }]
            },
        }
    }

    # Iterate over the dict above and fill in the blanks:
    # 1. Hit BitGo to get the state of the indexer
    # 2. Hit a public block explorer to compare chainheads
    # 3. Add the state to the dict
    for coin_symbol, coin_data in output_data['indexers'].items():
        print('*************************')
        print(coin_symbol.upper())
        print('*************************')
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
            except (ConnectionError, ReadTimeout):
                env_data['status'] = False
                env_data['latestBlock'] = 'IMS Unresponsive'
                env_data['blocksBehind'] = 'IMS Unresponsive'
                continue

            print(env_data['bgURL'])
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

            # Get the api handler class from the config
            api_handler_class = env_data.pop('apiHandler')
            if api_handler_class is None:
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
            if env_data['network'] == 'Dev':
                public_block_explorer_height = coin_data['environments'][1].get('referenceBlock', 0)
            else:
                api_handler = api_handler_class()
                try:
                    public_block_explorer_height = api_handler.get_url_and_return_height(env_data['publicURL'])
                except Exception as e:
                    print('Exception: {}'.format(e))
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

# lambda_handler(0,0)
