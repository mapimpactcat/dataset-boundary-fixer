'''A python script for selecting all the rows with duplicate values in a postgres database'''

import argparse
import csv
import itertools
import logging
import os
import sys

from dotenv import load_dotenv
import psycopg
import geopandas as gpd
import shapely

# Create logger for module.
logger = logging.getLogger(__name__)

# The log format.
LOG_FORMAT = '[%(levelname)-s] %(asctime)s %(funcName)s: %(message)s'

# The date format.
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# The batch size for fetching hex_ids from the database.
HEX_ID_BATCH_SIZE = 100000

# The batch size for selecting rows by hex_id.
ROWS_BATCH_SIZE = 10000


def get_hex_ids(conn):
    '''Get all hex ids, along with the count for each one'''
    hex_id_count = dict()
    with conn.cursor('duplicate_finder_cursor') as cursor:
        cursor.itersize = HEX_ID_BATCH_SIZE

        cursor.execute('''
            SELECT hex_id FROM national_dataset
        ''')

        count = 0
        for record in cursor:
            hex_id = record[0]
            hex_id_count[hex_id] = hex_id_count.get(hex_id, 0) + 1

            count += 1
            if count % HEX_ID_BATCH_SIZE == 0:
                logger.info('Got %d', count)

    return hex_id_count


def find_duplicates(hex_id_count):
    '''
    Find all duplicates (entries with a count > 0) for a dictionary of id -> count,
    and return them as pairs
    '''
    duplicates = []
    duplicates_found = 0
    for hex_id, count in hex_id_count.items():
        if count > 1:
            duplicates.append((hex_id, count))

            duplicates_found += 1
            if duplicates_found % HEX_ID_BATCH_SIZE == 0:
                logger.info('Got %d duplicates', count)

    return duplicates


def write_pairs_csv(filename, items):
    with open(filename, 'w', encoding='utf-8') as f:
        for a, b in items:
            f.write('{0},{1}\n'.format(a, b))


def read_pairs_csv(filename):
    pairs = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            a = row[0]
            b = int(row[1])
            pairs.append((a, b))

    return pairs


def get_rows_by_hex_id(conn, hex_ids):
    rows = []
    colnames = None

    count = 0
    total_count = len(hex_ids)

    with conn.cursor('duplicate_finder_cursor') as cursor:
        cursor.itersize = ROWS_BATCH_SIZE

        for batch in itertools.batched(hex_ids, ROWS_BATCH_SIZE):
            # Build query.
            batch_ids = '\'' + '\',\''.join(batch) + '\''
            query = 'SELECT * FROM national_dataset WHERE hex_id IN ({})'.format(batch_ids)
            # Execute query.
            cursor.execute(query)

            # If we don't already have them, read the column names.
            if colnames is None:
                colnames = [desc[0] for desc in cursor.description]

            # Read rows and append them to `rows`.
            for record in cursor:
                rows.append(record)

                count += 1
                if count % ROWS_BATCH_SIZE == 0:
                    logger.info('Got %d/%d', count, total_count)

    return (colnames, rows)


def main(args):
    # Connect to postgres database.
    logger.info('Connecting to postgres...')
    database_url = os.getenv('DATABASE_URL')
    try:
        conn = psycopg.connect(database_url)
        logger.info('Connected to postgres')
    except Exception as e:
        logger.error('Failed to connect to postgres database: %s', e)
        sys.exit(1)

    # Get duplicate hex ids.
    duplicates = []
    if args.read_cached_duplicates:
        # Read the cached duplicates file if specified.
        logger.info('Reading duplicate hex ids from csv file...')
        duplicates = read_pairs_csv('duplicate_hex_ids.csv')
    else:
        # Otherwise, get them from the DB (slower).
        # Get all hex ids and their counts as a dict.
        logger.info('Getting all hex ids and their counts from the database...')
        hex_id_count = get_hex_ids(conn)

        # Write all hex_ids and counts to file.
        logger.info('Writing all hex_ids along with their counts to hex_ids.csv')
        write_pairs_csv(hex_id_count.items())
        with open('hex_ids.csv', 'w', encoding='utf-8') as f:
            for hex_id, count in hex_id_count.items():
                f.write('{0},{1}\n'.format(hex_id, count))

        # Find all hex_ids that are duplicate.
        logger.info('Finding duplicate hex_ids...')
        duplicates = find_duplicates(hex_id_count)

        # Write all duplicate hex_ids to a file too.
        logger.info('Writing duplicate hex_ids to duplicate_hex_ids.txt')
        write_pairs_csv('duplicate_hex_ids.csv', duplicates)

    # Read all hexes from database.
    logger.info('Reading all duplicate hex rows from database...')
    hex_ids = list(map(lambda x: x[0], duplicates))
    column_names, rows = get_rows_by_hex_id(conn, hex_ids)

    # Convert all geometries to shapely geometries.
    logger.info('Converting geometries from WKB to shapely geometries')
    geom_idx = column_names.index('geom')
    geoms = list(map(lambda x: shapely.from_wkb(x[geom_idx]), rows))

    # Add to GeoDataFrame and write to gpkg.
    logger.info('Creating GeoDataFrame from rows')
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:27700")

    # Name columns in the data frame, and drop the duplicate 'geom' column containing wkb.
    gdf.rename(columns=dict(enumerate(column_names)), inplace=True)
    gdf.drop(columns=['geom'], inplace=True)

    # Write duplicates to gpkg.
    gdf.to_file('duplicates.gpkg', engine='pyogrio')


if __name__ == '__main__':
    # Load environment variables from .env file.
    load_dotenv()

    # Configure logging.
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(formatter)

    root.addHandler(handler)

    # Read command line args.
    parser = argparse.ArgumentParser(prog='duplicate_finder',
                                     description='find duplicate hexes in our national dataset',
                                     epilog='this duplicate_finder has super cow powers')
    parser.add_argument('--read-cached-duplicates',
                        action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    # Call main.
    main(args)
