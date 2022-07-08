#!/usr/bin/env python3
import os
import json
import argparse
import logging

from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions

logger = logging.getLogger("target-blob-storage")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def upload(args):
    logger.info(f"Exporting data...")
    config = args.config

    container_name = config.get('container')
    target_path = config.get('path_prefix')
    local_path = config.get('input_path')
    connect_string = config.get('connect_string')
    
    if connect_string:
        blob_service_client = BlobServiceClient.from_connection_string(connect_string)
    else:
        account_name = config.get('account_name')
        account_key = config.get('account_key')

        sas_token = generate_account_sas(
            account_name = account_name,
            account_key = account_key,
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(read=True,add=True,create=True,write=True,delete=True,list=True),
            expiry=datetime.utcnow() + timedelta(hours=1))

        blob_service_client = BlobServiceClient(account_url=target_path, credential=sas_token)

    for root, dirs, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            remote_file_path = file_path.replace(local_path, target_path)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=remote_file_path)

            # Upload the created file
            with open(file_path, "rb") as data:
                logger.debug(f"Uploading: {container_name}:{remote_file_path}")
                blob_client.upload_blob(data)

    logger.info(f"Data exported.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
