#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:33:46 2019

@author: Stefan Fritsch
"""

import yaml

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots
from debug import debug_title, debug_heartbeat


def cronjob(_source, _prefix, _retain):
    location = _source["location"]
    source_type = _source["type"]
    create_snapshot = globals()[source_type + "_create_snapshot"]
    delete_old_snapshots = globals()[source_type + "_delete_old_snapshots"]
    
    create_snapshot(location, _prefix)
    
    delete_old_snapshots(location, _prefix, _retain)


with open("config.yaml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


schedules = config["schedules"]
source_definitions = config["sources"]

for schedule in schedules:
    sources = schedule["sources"]
    
    for source in sources:
        cronjob(_source = source_definitions[source],
                _prefix = schedule["prefix"],
                _retain = schedule["retain"])
