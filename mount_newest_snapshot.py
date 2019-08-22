#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:33:46 2019

@author: Stefan Fritsch
"""

##### Imports #####

import os
import logging
import logging.config
import yaml

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots, cephfs_mount_newest_snapshot
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots, rbd_mount_newest_snapshot

from argparse import ArgumentParser
from sys import stdout

##### Parse command line #####

parser = ArgumentParser(description = "Create regular snapshots of volumes.")
parser.add_argument("-l", "--log-level",
                    help = "What severity of events should be locked. Can be one of DEBUG, INFO, WARN, ERROR, CRITICAL",
                    dest = "log_level",
                    default = "INFO",
                    type = str)
parser.add_argument("-c", "--config-file",
                    help = "Where to look for the config yaml?",
                    dest = "config_file",
                    default = "config.yaml",
                    type = str)
parser.add_argument("-t", "--schedule",
                    help = "The snapshots of which schedule should we mount, i.e. which snapshot prefix?",
                    dest = "schedule",
                    default = "daily",
                    type = str)
parser.add_argument("-s", "--source",
                    help = "Which backup source should we mount. One of the sources-entries in the config file",
                    dest = "source",
                    default = "",
                    type = str)

args = parser.parse_args()


##### Functions #####

def mount_newest_snapshot(_source_name, _schedule_name, _logger = main_logger):
    source = sources[_source_name]
    location = source["location"]
    mount_location = os.path.join("/mnt_backup", _source_name, _schedule_name)
    
    mount_newest_snapshot_type = globals()[source["type"] + "_mount_newest_snapshot"]
    
    mount_newest_snapshot_type(location, _schedule_name, mount_location, _logger)


##### The logger #####

formatter = logging.Formatter('[%(asctime)s][%(levelname)s]%(name)s %(message)s')
stdout_handler = logging.StreamHandler(stdout)
stdout_handler.setFormatter(formatter)

main_logger = logging.getLogger("[Main]")
main_logger.addHandler(stdout_handler)
main_logger.setLevel(args.log_level)

##### The config #####

with open(args.config_file, 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        main_logger.error(exc)


##### Set the schedules #####

sources = config["sources"]
mount_newest_snapshot(args.source, args.schedule)
