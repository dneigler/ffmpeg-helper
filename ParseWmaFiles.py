#!/usr/bin/python
# from pymongo import MongoClient

# client = MongoClient()
# db = client.test

import os, sys, subprocess, shlex, re, json
import fnmatch
import logging
from subprocess import call
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.music_mongodb
music_records = db.music_records
logging.basicConfig(filename='parse_wma_files.log', level=logging.DEBUG)
logger = logging.getLogger()

music_path = "/Volumes/DATA/ServerFolders/Music"  # /Abbey Lincoln"  # A\ Turtle\'s\ Dream
music_records.create_index([('format.filename', pymongo.ASCENDING)], unique=True)


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
    logger.debug(music_records.find_one(query))
    result = music_records.replace_one(query, p, True)
    logger.debug('One post: {0}'.format(result))


logger.info('Starting the Run')
for root, dirs, files in os.walk(music_path):
    path = root.split(os.sep)
    logger.debug((len(path) - 1) * '--- ' + os.path.basename(root))
    for file in files:
        logger.debug(len(path) * '--- ' + file + ' ' + os.path.join(root, file))
        if file.endswith(".wma"):
            probe_file(os.path.join(root, file))

logger.info('Completed Run')
