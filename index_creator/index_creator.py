#!/usr/bin/env python
import os
import sys
import logging
import argparse
from argparse import Namespace

from elasticsearch import Elasticsearch
import ast


def create_index(client, index_name, shards, replicas, mapping_dictionary):
    body_dict={}
    settings_dictionary = {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": replicas
        }
    }
    body_dict.update(settings_dictionary)
    body_dict.update(mapping_dictionary)
    response = client.indices.create(index=index_name, body=body_dict, ignore=400)
    return response


def read_mapping(filename):
    with open(filename,'r') as f:
        s = f.read()
        mapping = ast.literal_eval(s)
        return mapping


def main():
    logger.info('Starting execution...')
    mapping = {}
    if args.mapping_file is not None:
        mapping = read_mapping(args.mapping_file)
    target_host = args.host + ':' + args.port
    elastic = Elasticsearch(hosts=[target_host])
    result = create_index(elastic, args.index_name, args.shards, args.replicas, mapping)


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
                        help='Index name')
    parser.add_argument('-m', '--mapping', dest='mapping_file',
                        help="File containing the index mapping description in JSON format")
    parser.add_argument('-s', '--shards', dest='shards', type=int, default=1,
                        help="Number of index shards")
    parser.add_argument('-r', '--replicas', dest='replicas', type=int, default=0,
                        help="Number of index replicas")

    args: Namespace = parser.parse_args()

    main()
    sys.exit(0)