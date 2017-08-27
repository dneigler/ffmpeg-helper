#!/usr/bin/python
# from pymongo import MongoClient

# client = MongoClient()
# db = client.test

import os, sys, subprocess, shlex, re, json
import fnmatch
from subprocess import call
import pymongo
from pymongo import MongoClient
client = MongoClient()
db = client.music_mongodb
music_records = db.music_records

def probe_file(filename):
    # os.chdir(os.path.expanduser("~/ffmpeg"))
    # os.chdir('ffmpeg')
    ffprobe_path = './ffprobe'
    # ffprobe_path = os.path.expanduser("~/ffmpeg") + '/ffprobe
    cmnd = [ffprobe_path, '-v', 'quiet', '-print_format', 'json', '-show_format', '-hide_banner', filename]
    # p = subprocess.Popen(cmnd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = json.loads(subprocess.check_output(cmnd, stderr=subprocess.STDOUT))
    print(p)
    result = music_records.insert_one(p)
    print('One post: {0}'.format(result.inserted_id))

    # print(filename)
    # out, err =  p.communicate()
    # print("==========output==========")
    # print(out)
    # if err:
    #     print("========= error ========")
    #     print(err)

music_path = "/Volumes/DATA/ServerFolders/Music/Abbey Lincoln" # A\ Turtle\'s\ Dream

for root, dirs, files in os.walk(music_path):
    path = root.split(os.sep)
    print((len(path) - 1) * '---', os.path.basename(root))
    for file in files:
        print(len(path) * '---', file, os.path.join(root, file))
        if file.endswith(".wma"):
            probe_file(os.path.join(root, file))
