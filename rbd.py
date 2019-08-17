import os
import re
import datetime
import subprocess
from debug import debug_title, debug_heartbeat

def rbd_create_snapshot(_location, _prefix):
    debug_title("Creating snapshot for location: " + _location + " Prefix: " + _prefix)
    
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    snap_name = _prefix + "_-_" + now
    command = f'rbd snap create {_location}@{snap_name}'
    
    try:
        subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    except OSError:
        print (f'Creation of the snapshot {snap_name} failed')
    except ValueError as ve:
        print(ve)
    else:
        print (f'Successfully created the snapshot {snap_name}')


def rbd_list_snapshots(_location, _prefix):
    command = f'rbd snap ls {_location}'
    all_snaps_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).stdout.read()
    all_snaps_output = all_snaps_cmd.decode("utf-8").split("\n")
    
    prefix_snaps_full = [f 
                         for f in all_snaps_output
                         if re.search(_prefix, f)]
    prefix_snaps = [re.search(fr'{_prefix}[^\s]*', p).group(0) 
                    for p in prefix_snaps_full]
    
    return prefix_snaps


def rbd_delete_old_snapshots(_location, _prefix, _n_retain):
    debug_title("Deleting snapshots for location: " + _location + " Prefix: " + _prefix)
    
    snaps = rbd_list_snapshots(_location, _prefix)
    
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
            command = f'rbd snap rm {_location}@{snap}'
            subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
        except OSError:
            print ("Deletion of snapshot %s failed" % snap)
        except ValueError as ve:
            print(ve)
        else:
            print ("Successfully deleted snapshot %s " % snap)


def rbd_mount_newest_snapshot(_location, _prefix, _mount_base = "/cephrbd"):
    snaps = rbd_list_snapshots(_location, _prefix)
    newest_snapshot = sorted(snaps, reverse = True)[0]
    
    ## protect snap
    command = f'rbd snap protect {_location}@{newest_snapshot}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## clone snap
    command = f'rbd clone {_location}@{newest_snapshot} {_location}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## map clone
    command = f'rbd map {_location}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).stdout.read()
    newest_snap_device = newest_snap_cmd.decode("utf-8").strip()
    
    ## mount clone
    mountpoint = os.path.join(_mount_base, _location)
    os.makedirs(mountpoint, exist_ok = True)
    command = f'mount {newest_snap_device} {mountpoint}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    return newest_snapshot


def rbd_unmount_newest_snapshot(_location, _newest_snapshot, _mount_base = "/cephrbd"):
    ## unmount clone
    mountpoint = os.path.join(_mount_base, _location)
    command = f'umount {mountpoint}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## unmap clone
    command = f'rbd unmap {_location}_backup'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## delete clone
    command = f'rbd rm {_location}_backup'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## unprotect snap
    command = f'rbd snap unprotect {_location}@{_newest_snapshot}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()

