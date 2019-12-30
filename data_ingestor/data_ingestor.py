#!/usr/bin/env python
import os
import sys
import logging
import datetime
import argparse
from argparse import Namespace
from datetime import timedelta
from elasticsearch import Elasticsearch


EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)
FRACTIONS_PER_SECOND = 65536


def get_instrument_from_apid(apid):
    apid_table = [
        ('sc', range(168, 169)),
        ('epd', range(800, 912)),  ('epd', range(1600, 1616)),
        ('eui', range(912, 1008)),
        ('mag', range(1008, 1072)),
        ('met', range(1072, 1152)),
        ('phi', range(1152, 1200)),
        ('rpw', range(1200, 1312)),
        ('shi', range(1312, 1360)),
        ('spi', range(1360, 1440)), ('spi', range(1616, 1680)),
        ('stx', range(1440, 1520)),
        ('swa', range(1520, 1600))]

    for tuple in apid_table:
        if int(apid) in tuple[1]:
            return tuple[0]

    raise Exception('Unknown APID')


def get_datetime_from_obt(obt_string):
    num_of_seconds_since_epoch = int(obt_string[0:8], 16)
    fractions = int(obt_string[8:], 16)
    nanoseconds = fractions * pow(10, 9) / FRACTIONS_PER_SECOND
    milliseconds = int(nanoseconds / pow(10, 6))
    obt_datetime = EPOCH + timedelta(seconds=num_of_seconds_since_epoch) + timedelta(milliseconds=milliseconds)
    return obt_datetime.strftime("%Y-%m-%dT%H:%M:%S.{}".format(milliseconds))


def build_packet_json(packet_name, packet_content):
    pktm, apid, obt, no_sync_flag, destination_id, ssc_type, psubtype, scos2k_pi1, *unexpected = packet_name.split('_')
    ssc, ptype = ssc_type.split('.')
    no_sync_flag_boolean = 'false' if no_sync_flag == 0 else 'true'
    converted_obt = get_datetime_from_obt(obt)

    try:
        packet_dict = {
            'timestamp': datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            'instrument': get_instrument_from_apid(apid),
            'apid': apid,
            'destination_id': destination_id,
            'obt': converted_obt,
            'no_sync_flag': no_sync_flag_boolean,
            'ssc': ssc,
            'packet_type': ptype,
            'packet_subtype': psubtype,
            'scos2000_pi1': scos2k_pi1,
            'spacecraft': True,
            'content': packet_content
        }

        return packet_dict

    except:
        logging.error('Error when building packet json {}'.format(packet_name))


def ingest_file(path, packet_file_name):
    logging.info("Ingesting packet " + packet_file_name)
    full_path_to_file = path + os.sep + packet_file_name
    with open(full_path_to_file, 'r') as f:
        packet_content = f.read()
        packet_json = build_packet_json(packet_file_name, packet_content)
        logging.info(packet_json)
        # See https://stackoverflow.com/questions/51724443/elasticsearch-doc-type-and-parent-id
        elastic_conn.index(index=args.index_name, refresh=False, body=packet_json, doc_type='_doc')


def ingest_directory(data_dir):
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.startswith('PKTM'):
                ingest_file(root, file)


def alt_main():
    global elastic_conn
    if elastic_conn is None:
        elastic_conn = Elasticsearch([args.host],
                                     port=args.port,
                                     sniff_on_start=False,            # TODO False because using Docker
                                     sniff_on_connection_fail=False,  # TODO False because using Docker
                                     maxsize=10,
                                     http_compress=True)

    if args.directory:
        ingest_directory(args.directory)
    elif args.packetfile:
        ingest_file(args.packetfile)
    else:
        print("Either a file or directory was expected. None of them was provided. Exiting...")
        sys.exit(1)

##
#  Entry point
##


elastic_conn = None
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

    alt_main()
    sys.exit(0)