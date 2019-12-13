# Overview
An AWS Lambda function definition responsible for polling BitGo's indexer infrastructure for status and persisting that status as JSON files on s3.

This is the "back-end" portion of a (slightly) larger system that powers a web based dashboard that shows users that state of BitGo's indexer infrastructure.

# How It Works

1. The Lambda fcn defined in this project, polls BitGo's indexers (prod + test) to find the latest block processed for each coin.
2. It compares those values to public block explorers to determine whether or not we are behind chain head.
3. It stores the status of each of our indexers in a JSON file on s3.
4. The front-end app (this repo) fetches the most recent status file from s3 and uses it to construct a dashboard for the user.

# References
The paired, front-end project (the project that consumes the JSON data that this project builds) is available here: https://github.com/cooncesean/bg-indexer-health-front-end. They were distinct enough that it didn't make a whole lot of sense to smush them together.

# Public URL
This project is currently up and running at: http://bitgo-indexer-health-front-end.s3-website-us-west-2.amazonaws.com/
