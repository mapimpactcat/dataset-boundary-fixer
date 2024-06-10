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
from shapely.ops import transform
import h3
import pyproj

# Create logger for module.
logger = logging.getLogger(__name__)

# The log format.
LOG_FORMAT = '[%(levelname)-s] %(asctime)s %(funcName)s: %(message)s'

# The date format.
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


wgs84 = pyproj.CRS('EPSG:4326')
bng = pyproj.CRS('EPSG:27700')
project = pyproj.Transformer.from_crs(wgs84, bng, always_xy=True).transform


def append_polys(geom, geom_list):
    '''Flatten and append polygons to list, splitting MultiPolygons etc'''
    if geom.geom_type == 'Polygon':
        if not geom.is_empty:
            geom_list.append(geom)
    elif geom.geom_type == 'MultiPolygon' or geom.geom_type == 'GeometryCollection':
        for geom in geom.geoms:
            append_polys(geom, geom_list)


def keep_only(geom):
    '''
    Process a given shapely geometry and keep only polygons, returning an empty polygon if the
    geometry has no area
    '''
    # If the geometry is a GeometryCollection, convert it to a MultiPolygon and drop other types
    # of geometry.
    if geom.geom_type == 'GeometryCollection':
        polygons = []
        append_polys(geom, polygons)
        return shapely.MultiPolygon(polygons)

    # Otherwise, if the geometry is not a Polygon or MultiPolygon, just return an empty Polygon.
    if geom.geom_type != 'Polygon' and geom.geom_type != 'MultiPolygon':
        return shapely.Polygon()

    return geom


def cell_to_shapely(cell):
    coords = h3.h3_to_geo_boundary(cell)
    flipped = tuple(coord[::-1] for coord in coords)
    return shapely.Polygon(flipped)


def deduplicate(rows):
    if len(rows) == 1:
        return rows.iloc[0]

    # Get h3 hex id.
    hex_id = rows.iloc[0].hex_id
    hex_geom = transform(project, cell_to_shapely(hex_id))

    # Find row with largest area and copy it.
    largest_area_row = rows.iloc[0]
    largest_area = largest_area_row.geometry.area

    for idx in range(0, len(rows)):
        other = rows.iloc[idx]

        if not other.evi:
            logger.info('Null value for evi with hex id %s, idx %d', hex_id, idx)
            continue

        other_area = other.geometry.area
        if other_area > largest_area:
            largest_area_row = other
            largest_area = other_area

    # Copy row and update geometry.
    row = largest_area_row.copy()
    row.geometry = hex_geom

    return row


def main(args):
    # Read input file.
    logger.info('Reading duplicates.gpkg...')
    gdf = gpd.read_file('duplicates.gpkg', engine='pyogrio')

    # Group by hex id and deduplicate.
    logger.info('Grouping by hex id and deduplicating...')
    rows = []
    for a, b in gdf.groupby('hex_id'):
        rows.append(deduplicate(b))

    # Create new GDF.
    logger.info('Creating deduplicated GeoDataFrame')
    deduplicated = gpd.GeoDataFrame(rows, crs=gdf.crs)

    # Write to new file.
    logger.info('Writing to deduplicated.gpkg')
    deduplicated.to_file('deduplicated.gpkg', engine='pyogrio')


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
    parser = argparse.ArgumentParser(prog='duplicate_fixer',
                                     description='merges duplicate hexes by hex id',
                                     epilog='this duplicate_fixer has super cow powers')

    args = parser.parse_args()

    # Call main.
    main(args)
