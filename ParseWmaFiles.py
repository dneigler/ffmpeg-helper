#!/usr/bin/python

import argparse
import json
import logging
import os
import subprocess

import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.music_mongodb
music_records = db.music_records

music_path = "/Volumes/DATA/ServerFolders/Music"  # /Abbey Lincoln"  # A\ Turtle\'s\ Dream
# music_path = "Abbey Lincoln" # /A Turtle's Dream"
music_path = "/Volumes/MacbookHD2/eMusic"

parser = argparse.ArgumentParser(description='Document audio files into an easy to query database.')
parser.add_argument('-r', '--rebuild', help='Rebuilds database indexes.', action="store_true")
parser.add_argument('-p', '--parse', help='Skips parsing audio files.', action="store_true")
parser.add_argument('-a', '--audiopath', nargs='?', action='store', default=music_path, const=music_path)
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-f", "--format",
                    help="Format database before parsing anything - completely clears all data and rebuilds.",
                    action="store_true")

args = parser.parse_args()
if args.verbose:
    logging.basicConfig(filename='parse_wma_files.log', level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
else:
    logging.basicConfig(filename='parse_wma_files.log', level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

logger = logging.getLogger(__name__)

music_path = args.audiopath

if args.format:
    logger.info("Formatting database...")
    music_records.delete_many({})

music_records.create_index([('format.filename', pymongo.ASCENDING)], unique=True)

# Mongodb aggregations
pipeline_format = [
    { "$group": {"_id": "$format.format_long_name", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "format_tracks"}
]

pipeline_artist = [
    {"$group": {"_id": "$format.tags.album_artist", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "artist_tracks"}
]

pipeline_album = [
    { "$group": {"_id": "$format.tags.album", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "album_tracks"}
]

pipeline_song = [
    {"$group": {"_id": "$format.tags.title", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    {"$out": "song_tracks"}
]

pipeline_artist_albums = [
    # { "$unwind": "$format.tags" },
    {
        "$group": {
            "_id": {
                "artist": "$format.tags.album_artist",
                "album": "$format.tags.album"
            },
            "tracks": {
                "$push": "$$ROOT"
            },
            "track_count": {
                "$sum": 1
            }
        }
    },
    { "$out": "artist_album_tracks_raw"}
]

pipeline_artist_albums_2 = [
    {
        "$group": {
            "_id": "$_id.artist",
            "album_count": { "$sum": 1},
            "albums": {
                "$push": {
                    "album": "$_id.album", "tracks": "$tracks"
                }
            }
        }
    },
    { "$out": "artist_album_tracks"}
]


def artist_to_album_mapper(track_json):
    logger.debug(track_json)
    return {'album_artist': track_json.tags.album_artist, 'album': track_json.tags.album}

def probe_file(filename):
    ffprobe_path = './ffprobe'
    # ffprobe_path = os.path.expanduser("~/ffmpeg") + '/ffprobe
    cmnd = [ffprobe_path, '-v', 'quiet', '-print_format', 'json', '-show_format', '-hide_banner', filename]
    # p = subprocess.Popen(cmnd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = json.loads(subprocess.check_output(cmnd, stderr=subprocess.STDOUT))

    record_filename = p['format']['filename']
    logger.debug('record-filename: ' + record_filename)
    logger.debug(p)
    query = {'format.filename': record_filename}

    result = music_records.replace_one(query, p, True)
    logger.debug('One post: {0}'.format(result))
    # pprint(p) # ['format']['tags'])

def rebuild_database():
    db.music_records.aggregate(pipeline_format)
    db.music_records.aggregate(pipeline_artist)
    db.music_records.aggregate(pipeline_album)
    db.music_records.aggregate(pipeline_artist_albums)
    db.artist_album_tracks_raw.aggregate(pipeline_artist_albums_2)
    db.music_records.aggregate(pipeline_song)

logger.info('Starting the Run')
logger.info(args)

if args.parse:
    logger.info('Parsing audio files in {0}'.format(music_path))
    for root, dirs, files in os.walk(music_path):
        path = root.split(os.sep)
        logger.debug((len(path) - 1) * '--- ' + os.path.basename(root))
        for file in files:
            logger.debug(len(path) * '--- ' + file + ' ' + os.path.join(root, file))
            if file.endswith(".wma") or file.endswith(".mp3") or file.endswith(".flac"):
                probe_file(os.path.join(root, file))

if args.rebuild or args.parse:
    logger.info('Rebuilding database')
    rebuild_database()

logger.info('Completed Run, db has {0} records'.format(music_records.find({}).count()))
