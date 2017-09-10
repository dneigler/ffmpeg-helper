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
            # "track_count": { "$push": "$track_count" },
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
    # os.chdir(os.path.expanduser("~/ffmpeg"))
    # os.chdir('ffmpeg')
    ffprobe_path = './ffprobe'
    # ffprobe_path = os.path.expanduser("~/ffmpeg") + '/ffprobe
    cmnd = [ffprobe_path, '-v', 'quiet', '-print_format', 'json', '-show_format', '-hide_banner', filename]
    # p = subprocess.Popen(cmnd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = json.loads(subprocess.check_output(cmnd, stderr=subprocess.STDOUT))

    record_filename = p['format']['filename']
    logger.debug('record-filename: ' + record_filename)
    logger.debug(p)
    # {'format': {'filename': "/Volumes/DATA/ServerFolders/Music/Abbey Lincoln/A Turtle's Dream/01_Throw It Away.wma"
    query = {'format.filename': record_filename}
    logger.debug('mongo findOne: ')
    # logger.debug(music_records.find_one(query))

    result = music_records.replace_one(query, p, True)
    logger.debug('One post: {0}'.format(result))

    # logger.debug('p: {0}'.format(p))

    # pprint(p) # ['format']['tags'])
    # pprint(len(p.keys()))

    db.music_records.aggregate(pipeline_format)
    db.music_records.aggregate(pipeline_artist)
    db.music_records.aggregate(pipeline_album)
    db.music_records.aggregate(pipeline_artist_albums)
    db.artist_album_tracks_raw.aggregate(pipeline_artist_albums_2)
    # nothing to group until its in the db
    # for key, group in itertools.groupby(p, lambda x: x['format']['tags']['album_artist']):
    #     for thing in group:
    #         print("A %s is a %s.".format(thing, key))
    #
    # album_groups = itertools.groupby(p, artist_to_album_mapper)
    # db.albums.insert(album_groups)


logger.info('Starting the Run')
for root, dirs, files in os.walk(music_path):
    path = root.split(os.sep)
    logger.debug((len(path) - 1) * '--- ' + os.path.basename(root))
    for file in files:
        logger.debug(len(path) * '--- ' + file + ' ' + os.path.join(root, file))
        if file.endswith(".wma") or file.endswith(".mp3") or file.endswith(".flac"):
            probe_file(os.path.join(root, file))

mapper = Code("""
function() {
    emit(this.format.tags.album_artist, {title: this.format.tags.title, album: this.format.tags.album,  album_artist: this.format.tags.album_artist, all: this});
}
""")

reducer = Code("""function(key,values) {
    var result = {};
    var commentFields = {
        "album": '',
        "title": '', 
        "all": ''
    };
    // "album_artist": '',
    // 

    values.forEach(function(value) {
        var field;
        var albumValue;
        if ("title" in value) {
            if (!("albums" in result)) {
                result.albums = [];
            }
            result.albums.forEach(function (a) {
                if (a.album == value.album) {
                    if (!("albumObject" in a)) {
                        a.albumObject = {};
                    }
                    if (!("songs" in a.albumObject)) {
                        a.albumObject.songs = [];
                    }
                    a.albumObject.songs.push(value);
                }
            });
            result.albums.push(value);
        } else if ("albums" in value) {
            if (!("albums" in result)) {
                result.albums = [];
            }
            result.albums.push.apply(result.albums, value.albums);
        }
        for (field in value) {
            if (value.hasOwnProperty(field) && !(field in commentFields)) {
                result[field] = value[field];
            }
        }
    });
    return result;
    }
""")

# values.forEach(function(value) {
#         var field;
#         if ("comment" in value) {
#             if (!("comments" in result)) {
#                 result.comments = [];
#             }
#     var total = 0;
#     for (var i = 0; i < values.length; i++) {
#         return values;
#         total += values[i];
#     }
#     return total;
# }

# results = music_records.map_reduce(mapper, reducer, out="titles")
# for doc in results.find():
#     logger.info(doc)

logger.info('Completed Run')
