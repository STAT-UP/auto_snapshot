#!/bin/bash

git reset --hard && git pull && chmod u+x auto_snapshot.py snapshotter.py redownload.bash

if [[ -e /secrets/ceph-backup/config.yaml && ! -e /auto_snapshot/config.yaml ]]; then
  ln -s /secrets/ceph-backup/config.yaml /auto_snapshot/config.yaml
fi
