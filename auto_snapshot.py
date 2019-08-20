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
import apscheduler.schedulers.blocking
import apscheduler.triggers.cron

from cephfs import cephfs_create_snapshot, cephfs_list_snapshots, cephfs_delete_old_snapshots, cephfs_mount_newest_snapshot
from rbd import rbd_create_snapshot, rbd_list_snapshots, rbd_delete_old_snapshots, rbd_mount_newest_snapshot

from argparse import ArgumentParser
from sys import stdout

##### Parse command line #####

parser = ArgumentParser(description = "Create regular snapshots of volumes.")
parser.add_argument("--heartbeat",
                    help = "Print a regular heartbeat as a debug message every minute",
                    action = "store_const",
                    dest = "heartbeat",
                    const = True,
                    default = False)
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

args = parser.parse_args()


##### Functions #####

def cronjob(_source, _prefix, _retain, _mount_newest, _mount_location = ""):
    location = _source["location"]
    source_type = _source["type"]
    
    logger = logging.getLogger("[_prefix][_source]")
    logger.addHandler(stdout_handler)
    
    create_snapshot = globals()[source_type + "_create_snapshot"]
    mount_newest_snapshot = globals()[source_type + "_mount_newest_snapshot"]
    delete_old_snapshots = globals()[source_type + "_delete_old_snapshots"]
    
    create_snapshot(location, _prefix, _logger = logger)
    
    if _mount_newest:
        mount_newest_snapshot(location, _prefix, _mount_location, _logger = logger)
    
    delete_old_snapshots(location, _prefix, _retain, _logger = logger)


##### The logger #####

formatter = logging.Formatter('[%(asctime)s][%(levelname)s]%(name)s %(message)s')
stdout_handler = logging.StreamHandler(stdout)
stdout_handler.setFormatter(formatter)
stdout_handler.setLevel(args.log_level)


##### The config #####

with open(args.config_file, 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


##### Set the schedules #####

schedules = config["schedules"]
source_definitions = config["sources"]

scheduler = apscheduler.schedulers.blocking.BlockingScheduler()

for schedule_name, schedule in schedules.items():
    sources = schedule["sources"]
    mount_newest = schedule.get('mount_newest', False)
    
    for source_name in sources:
        trigger = apscheduler.triggers.cron.CronTrigger(**(schedule["cron"]))
        
        scheduler.add_job(cronjob,
                          trigger,
                          kwargs = {
                              "_source": source_definitions[source_name],
                              "_prefix": schedule_name,
                              "_retain": schedule["retain"],
                              "_mount_newest": mount_newest,
                              "_mount_location": os.path.join("/mnt_backup", source_name, schedule_name)
                          })


##### A heartbeat #####

if args.heartbeat:
    trigger = apscheduler.triggers.cron.CronTrigger(second = 1)
    beat_logger = logging.getLogger(" Heartbeat ")
    beat_logger.addHandler(stdout_handler)

    print("beating")
    scheduler.add_job(lambda _logger : (_logger.debug(""), print("badum")) ,
                    trigger,
                    kwargs = {"_logger": beat_logger})

scheduler.start()
