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

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots, cephfs_mount_newest_snapshot, cephfs_mount_snapshot, cephfs_unmount_snapshot
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots, rbd_mount_newest_snapshot, rbd_mount_snapshot, rbd_unmount_snapshot

from argparse import ArgumentParser
from sys import stdout

##### Parse command line #####

parser = ArgumentParser(description = "Manipulate snapshots")
parser.add_argument("command",
                    help = "One of create, mount_newest, mount and unmount",
                    dest = "command",
                    choices = ["create", "mount_newest", "mount", "unmount"],
                    required = True,
                    type = str)
parser.add_argument("-l", "--log-level",
                    help = "What severity of events should be logged. Can be one of DEBUG, INFO, WARN, ERROR, CRITICAL",
                    dest = "log_level",
                    default = "DEBUG",
                    type = str)
parser.add_argument("-c", "--config-file",
                    help = "Where to look for the config yaml?",
                    dest = "config_file",
                    default = "config.yaml",
                    type = str)
parser.add_argument("-p", "--prefix",
                    help = "The prefix of the snapshot to be created or mounted. E.g. the name of the schedule.",
                    dest = "prefix",
                    default = "daily",
                    type = str)
parser.add_argument("-s", "--source",
                    help = "Which backup source should we mount. One of the sources-entries in the config file",
                    dest = "source",
                    default = "",
                    type = str)
parser.add_argument("-t", "--snapshot",
                    help = "Which snapshot - currently only used for mount",
                    dest = "snapshot",
                    default = "",
                    type = str)
parser.add_argument("--mount-dir",
                    help = "The directory to unmount. Only used for unmount",
                    dest = "mount_dir",
                    default = "",
                    type = str)
parser.add_argument("--pool",
                    help = "The pool of the image that should be unmounted. Only used for unmount of rbd images",
                    dest = "pool",
                    default = "replicapool",
                    type = str)

args = parser.parse_args()



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

##### Functions #####

def mount_newest_snapshot(_source_name, _prefix, _logger = main_logger):
    source = sources[_source_name]
    location = source["location"]
    mount_location = os.path.join("/mnt_backup", _source_name, _prefix)
    
    type_mount_newest_snapshot = globals()[source["type"] + "_mount_newest_snapshot"]
    
    type_mount_newest_snapshot(location, _prefix, mount_location, _logger)

def create_snapshot(_source_name, _prefix, _logger = main_logger):
    source = sources[_source_name]
    location = source["location"]
    create_snapshot = globals()[source["type"] + "_create_snapshot"]
    
    create_snapshot(location, _prefix, _logger)

def mount_snapshot(_source_name, _prefix, _snapshot, _logger = main_logger):
    source = sources[_source_name]
    location = source["location"]
    mount_location = os.path.join("/mnt_backup", _source_name, _prefix)
    
    type_mount_snapshot = globals()[source["type"] + "_mount_snapshot"]
    
    type_mount_snapshot(location, _snapshot, mount_location, _logger)

def unmount_snapshot(_source_name, _prefix, _logger = main_logger):
    source = sources[_source_name]
    mount_location = os.path.join("/mnt_backup", _source_name, _prefix)
    
    if source["type"] == "cephfs":
        cephfs_unmount_snapshot(mount_location, _logger)
    elif source["type"] == "rbd":
        pool = source["location"].split("/")[1]
        rbd_unmount_snapshot(mount_location, pool, _logger)
    else:
        raise Exception("Unknown type in source")

def unmount_dir(_mount_location, _type = "rbd", _pool = "replicapool", _logger = main_logger):

    if _type == "cephfs":
        cephfs_unmount_snapshot(_mount_location, _logger)
    elif _type == "rbd":
        rbd_unmount_snapshot(_mount_location, _pool, _logger)
    else:
        raise Exception("Unknown type passed to function")

##### execute #####
sources = config["sources"]

if args.command == "mount_newest":
    mount_newest_snapshot(args.source, args.prefix)
elif args.command == "create":
    create_snapshot(args.source, args.prefix)
elif args.command == "mount":
    mount_snapshot(args.source, args.prefix, args.snapshot)
elif args.command == "unmount":
    unmount_snapshot(args.mount_dir, args.pool)
else:
    raise Exception("Unknown command")