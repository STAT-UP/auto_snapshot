#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:33:46 2019

@author: Stefan Fritsch
"""

##### Imports #####

import sys
import os
import logging
import logging.config
import yaml

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots, cephfs_mount_newest_snapshot, cephfs_mount_snapshot, cephfs_unmount_snapshot
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots, rbd_mount_newest_snapshot, rbd_mount_snapshot, rbd_unmount_snapshot, rbd_get_image_info, rbd_remove_snapshot

from argparse import ArgumentParser
from sys import stdout

##### Parse command line #####

parser = ArgumentParser(description = "Manipulate snapshots")
parser.add_argument(help = "One of create, mount_newest, mount, unmount and unmount_rbd, remove_rbd_snapshot",
                    dest = "command",
                    choices = ["create", "mount_newest", "mount", "unmount", "unmount_rbd", "remove_rbd_snapshot"],
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
parser.add_argument("--device",
                    help = "A mapped rbd device. Only used for remove_rbd_snapshot",
                    dest = "device",
                    default = "",
                    type = str)
parser.add_argument("--pool",
                    help = "The pool of the image that should be unmounted. Only used for unmount of rbd images",
                    dest = "pool",
                    default = "replicapool",
                    type = str)
parser.add_argument("--log-file",
                    help = "Where to write logs?",
                    dest = "log_file",
                    default = "./snapshotter.log",
                    type = str)
args = parser.parse_args()



##### The logger #####

formatter = logging.Formatter('[%(asctime)s][%(levelname)s]%(name)s %(message)s')
stdout_handler = logging.StreamHandler(stdout)
stdout_handler.setFormatter(formatter)
file_handler = logging.FileHandler(args.log_file)
file_handler.setFormatter(formatter)

main_logger = logging.getLogger("[Main]")
main_logger.addHandler(stdout_handler)
main_logger.addHandler(file_handler)
main_logger.setLevel(args.log_level)

def exception_handler(type, value, tb):
    main_logger.exception("Uncaught exception: {0}".format(str(value)))

# Install exception handler
sys.excepthook = exception_handler


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
    
    mount_parent = os.path.join("/mnt_backup", _source_name)
    if not os.path.exists(mount_parent):
        os.makedirs(mount_parent)
    
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
    
    mount_parent = os.path.join("/mnt_backup", _source_name)
    if not os.path.exists(mount_parent):
        os.makedirs(mount_parent)
    
    mount_location = os.path.join("/mnt_backup", _source_name, _prefix)
    
    type_mount_snapshot = globals()[source["type"] + "_mount_snapshot"]
    type_mount_snapshot(location, _prefix, _snapshot, mount_location, _logger)

def unmount_snapshot(_source_name, _prefix, _logger = main_logger):
    source = sources[_source_name]
    mount_location = os.path.join("/mnt_backup", _source_name, _prefix)
    
    if source["type"] == "cephfs":
        cephfs_unmount_snapshot(mount_location, _logger)
    elif source["type"] == "rbd":
        pool = source["location"].split("/")[0]
        rbd_unmount_snapshot(_mount_location = mount_location, _pool = pool, _logger = _logger)
    else:
        raise Exception("Unknown type in source")

def unmount_dir(_mount_location, _type = "rbd", _pool = "replicapool", _logger = main_logger):
    if _type == "cephfs":
        cephfs_unmount_snapshot(_mount_location, _logger)
    elif _type == "rbd":
        rbd_unmount_snapshot(_mount_location, _pool, _logger)
    else:
        raise Exception("Unknown type passed to function")

def remove_rbd_snapshot(_dev, _pool = "replicapool", _logger = main_logger):
    info = rbd_get_image_info(_dev, _pool = _pool, _logger = _logger)
    rbd_remove_snapshot(_info = info, _logger = _logger)

##### execute #####
sources = config["sources"]

if args.command == "mount_newest":
    mount_newest_snapshot(_source_name = args.source, _prefix = args.prefix)
elif args.command == "create":
    create_snapshot(_source_name = args.source, _prefix = args.prefix)
elif args.command == "mount":
    mount_snapshot(_source_name = args.source, _prefix = args.prefix, _snapshot = args.snapshot)
elif args.command == "unmount":
    unmount_snapshot(_source_name = args.source, _prefix = args.prefix)
elif args.command == "unmount_rbd":
    unmount_dir(args.mount_dir, _type = "rbd", _pool = args.pool)
elif args.command == "remove_rbd_snapshot":
    remove_rbd_snapshot(_dev = args.device, _pool = args.pool)
else:
    raise Exception("Unknown command")