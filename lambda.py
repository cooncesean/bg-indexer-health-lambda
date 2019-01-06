import datetime
import json

from urllib.request import Request, urlopen

import boto3


def lambda_handler(event, context):
    """
    Runs through every indexer in BitGo's stack, compares it state to a public
    block explorer, and writes that state to a JSON file on s3 (actually, two
    JSON files, one time-stamped file and another that represents the "latest"
    file that a front-end app will pull from.)
    """
    current_time = datetime.datetime.now()

    indexer_data = {
        'BTC': {
            'foo': 'bar',
        },
        'LTC': {
            'foo': 'bzrtz'
        }
    }
    string = json.dumps(indexer_data)
    encoded_string = string.encode("utf-8")

    # Create the file names
    dated_file_name = "{}.json".format(current_time)
    latest_file_name = "latest.json"

    # Write the files to the bucket
    bucket_name = "bitgo-indexer-health"
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=dated_file_name, Body=encoded_string)
    s3.Bucket(bucket_name).put_object(Key=latest_file_name, Body=encoded_string)
