import os
import re
import datetime
import subprocess
from debug import debug_title, debug_heartbeat

def cephfs_create_snapshot(_location, _prefix):
    debug_title("Creating snapshot for Dir: " + _location + " Prefix: " + _prefix)
    
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    
    try:
        if not(os.path.exists(_location)):
            raise ValueError("Directory " + _location + " does not exist")
        path = os.path.join(_location, ".snap", _prefix + "_-_" + now)
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    except ValueError as ve:
        print(ve)
    else:
        print ("Successfully created the directory %s " % path)


def cephfs_list_snapshots(_location, _prefix):
    path = os.path.join(_location, ".snap")
    snapdirs = [f 
                for f in os.listdir(path) 
                if os.path.isdir(os.path.join(path, f)) 
                    and re.match(_prefix, f)]
    return snapdirs


def cephfs_delete_old_snapshots(_location, _prefix, _n_retain):
    debug_title("Deleting snapshots for Dir: " + _location + " Prefix: " + _prefix)
    
    snaps = cephfs_list_snapshots(_location, _prefix)
    
    print("All snapshots: ")
    print(snaps)
    
    sorted_snaps = sorted(snaps, reverse = True)
    if _n_retain > 0:
        keep = max(0, _n_retain - 1)
        del sorted_snaps[:keep]
    
    print("We want to delete: ")
    print(sorted_snaps)
    
    for snap in sorted_snaps:
        try:
            path = os.path.join(_location, ".snap", snap)
            if not(os.path.exists(path)):
                raise ValueError("Snapshot " + path + " does not exist")
            subprocess.Popen(f'rmdir {path}', shell = True, stdout = subprocess.PIPE).wait()
        except OSError:
            print ("Deletion of snapshot %s failed" % path)
        except ValueError as ve:
            print(ve)
        else:
            print ("Successfully deleted snapshot %s " % path)


def cephfs_newest_snapshot_path(_location, _snapshot_prefix):
    snaps = cephfs_list_snapshots(_location, _snapshot_prefix)
    newest_snapshot = sorted(snaps, reverse = True)[0]
    snapshot_path = os.path.join(_location, ".snap", newest_snapshot)
    
    return snapshot_path
