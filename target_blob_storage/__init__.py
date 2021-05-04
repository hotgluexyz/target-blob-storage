#!/usr/bin/env python3
import os
import json
import argparse
import logging

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

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
    container_name = config['container']
    target_path = config['path_prefix']
    local_path = config['input_path']
    connect_string = config['connect_string']

    # Upload all data in input_path to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connect_string)

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
