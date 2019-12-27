#!/usr/bin/env python
import os
import sys
import logging
import argparse
from argparse import Namespace

from elasticsearch import Elasticsearch

def main():
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, files in os.walk(args.directory):
        path = root.split(os.sep)
        print((len(path) - 1) * '---', os.path.basename(root))
        for file in files:
            print(len(path) * '---', file)


program = os.path.basename(sys.argv[0])
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('IndexCreator')

    program_description = """
            Create an index in Elasticsearch. 
            """
    parser = argparse.ArgumentParser(description=program_description)
    parser.add_argument('-H', '--host', dest='host', default="localhost",
                        help='Elasticsearch host. Defaults to localhost')
    parser.add_argument('-P', '--port', dest='port', default='9200',
                        help='Elasticsearch API port. Defaults to 9200')
    parser.add_argument('index_name', nargs=1, type=str,
                        help='Name of the index to use for ingesting packets')
    parser.add_argument('-d', '--directory', dest='directory', type=str, default="./",
                        help='Directory containing the packet files')
    parser.add_argument('-f', '--file', dest='packetfile', type=str,
                        help='Ingest only this file into ElasticSearch')
    args: Namespace = parser.parse_args()

    if args.directory and args.packetfile:
        print("-d and -f are mutually exclusive ...")
        sys.exit(2)

    print(args.directory)
    main()
    sys.exit(0)