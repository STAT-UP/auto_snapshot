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

def cephfs_list_snapshots(_dir, _prefix):
    path = os.path.join(_dir, ".snap")
    snapdirs = [f
                for f in os.listdir(path)
                if os.path.isdir(os.path.join(path, f))
                    and re.match(_prefix, f)]
    return snapdirs

def cephfs_newest_snapshot_path(_source, _definition):
    snapshots = cephfs_list_snapshots(_source["path"], _definition["snapshot_prefix"])
    newest_snapshot = sorted(snaps, reverse = True)[0]
    snapshot_path = os.path.join(_source["path"], ".snap", newest_snapshot)
    
    return snapshot_path
    

def rsync_transfer(_source, _definition, _additional_args = "--delete --progress"):
    rsync_skip_compress = '3fr/3g2/3gp/3gpp/7z/aac/ace/amr/apk/appx/appxbundle/arc/arj/arw/asf/avi/bz2/cab/cr2/crypt[5678]/dat/dcr/deb/dmg/drc/ear/erf/flac/flv/gif/gpg/gz/iiq/iso/jar/jp2/jpeg/jpg/k25/kdc/lz/lzma/lzo/m4[apv]/mef/mkv/mos/mov/mp[34]/mpeg/mp[gv]/msi/nef/oga/ogg/ogv/opus/orf/pef/png/qt/rar/rds/rpm/rw2/rzip/s7z/sfx/sr2/srf/svgz/t[gb]z/tlz/txz/vob/wim/wma/wmv/xlsx/xz/zip'
    
    snapshot_path = cephfs_newest_snapshot_path(_source, _definition)
    source_string = f'{_definition["user"]}@{_definition["server"]}:{snapshot_path}'
    target_string = os.path.join(_definition["local_path"], _source["path"])
    
    command = f'rsync -aAx --rsync-path="sudo rsync" --skip-compress={rsync_skip_compress} --numeric-ids {_additional_args} -e "ssh -p {_definition["port"]} -T -c aes128-gcm@openssh.com -x" {source_string} {target_string}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()


def cronjob(_source, _definition):
    rsync_transfer(_source, _definition)



with open("config.yaml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


transfers = config["transfers"]
source_definitions = config["sources"]

scheduler = apscheduler.schedulers.blocking.BlockingScheduler()

for transfer in transfers:
    sources = transfer["source"]
    
    for source in sources:
        trigger = apscheduler.triggers.cron.CronTrigger(**(transfer["cron"]))
        
        scheduler.add_job(cronjob,
                          trigger,
                          kwargs = {
                              "_source": source_definitions[source],
                              "_definition": transfer
                          })


trigger = apscheduler.triggers.cron.CronTrigger(second = 1)
scheduler.add_job(debug_heartbeat,
                  trigger)

scheduler.start()
