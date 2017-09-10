#!/usr/bin/python

import os, sys, subprocess, shlex, re, json
import fnmatch
import logging
from subprocess import call
from bson.code import Code
import itertools

from pprint import pprint
import pymongo
from pymongo import MongoClient
from bson.son import SON

client = MongoClient()
db = client.music_mongodb
music_records = db.music_records
logging.basicConfig(filename='parse_wma_files.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)

music_path = "/Volumes/DATA/ServerFolders/Music"  # /Abbey Lincoln"  # A\ Turtle\'s\ Dream
# music_path = "Abbey Lincoln" # /A Turtle's Dream"
music_path = "/Volumes/MacbookHD2/eMusic"
music_records.create_index([('format.filename', pymongo.ASCENDING)], unique=True)

# Mongodb aggregations
pipeline_format = [
    { "$group": {"_id": "$format.format_long_name", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "format_tracks"}
]

pipeline_artist = [
    { "$group": {"_id": "$format.tags.artist", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "artist_tracks"}
]

pipeline_album = [
    { "$group": {"_id": "$format.tags.album", "tracks": {"$push": "$$ROOT"}, "track_count": {"$sum": 1}}},
    { "$out": "album_tracks"}
]


pipeline_artist_albums = [
    # { "$unwind": "$format.tags" },
    {
        "$group": {
            "_id": {
                "artist": "$format.tags.artist",
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

    db.music_records.aggregate(pipeline_format)
    db.music_records.aggregate(pipeline_artist)
    db.music_records.aggregate(pipeline_album)
    db.music_records.aggregate(pipeline_artist_albums)
    db.artist_album_tracks_raw.aggregate(pipeline_artist_albums_2)

logger.info('Starting the Run')
for root, dirs, files in os.walk(music_path):
    path = root.split(os.sep)
    logger.debug((len(path) - 1) * '--- ' + os.path.basename(root))
    for file in files:
        logger.debug(len(path) * '--- ' + file + ' ' + os.path.join(root, file))
        if file.endswith(".wma") or file.endswith(".mp3") or file.endswith(".flac"):
            probe_file(os.path.join(root, file))

logger.info('Completed Run')
