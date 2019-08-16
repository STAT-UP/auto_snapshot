#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:33:46 2019

@author: Stefan Fritsch
"""

import yaml
import os
import re
import datetime
import apscheduler.schedulers.blocking
import apscheduler.triggers.cron
import subprocess

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots

def debug_title(_text):
    print("####################################################################")
    print("## " + _text)
    print("####################################################################")


def debug_heartbeat():
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    
    print(f"[{now}] Still alive")


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

scheduler = apscheduler.schedulers.blocking.BlockingScheduler()

for schedule in schedules:
    sources = schedule["source"]
    
    for source in sources:
        trigger = apscheduler.triggers.cron.CronTrigger(**(schedule["cron"]))
        
        scheduler.add_job(cronjob,
                          trigger,
                          kwargs = {
                              "_source": source_definitions[source],
                              "_prefix": schedule["prefix"],
                              "_retain": schedule["retain"]
                          })


trigger = apscheduler.triggers.cron.CronTrigger(second = 1)
scheduler.add_job(debug_heartbeat,
                  trigger)

scheduler.start()
    