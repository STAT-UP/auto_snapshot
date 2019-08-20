import os
import re
import datetime
import subprocess

def rbd_create_snapshot(_location, _prefix, _logger):
    _logger.info("rbd_create_snapshot: Creating snapshot")
    
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    snap_name = _prefix + "_-_" + now
    command = f'rbd snap create {_location}@{snap_name}'
    
    try:
        subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    except OSError:
        _logger.error(f'Creation of the snapshot {snap_name} failed')
    except ValueError as ve:
        _logger.error(ve)
    else:
        _logger.debug(f'Successfully created the snapshot {snap_name}')


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


def rbd_delete_old_snapshots(_location, _prefix, _n_retain, _logger):
    _logger.info("rbd_delete_old_snapshots: Deleting snapshots")
    
    snaps = rbd_list_snapshots(_location, _prefix)
    
    _logger.debug("All snapshots: ")
    _logger.debug(snaps)
    
    sorted_snaps = sorted(snaps, reverse = True)
    if _n_retain > 0:
        keep = max(0, _n_retain - 1)
        del sorted_snaps[:keep]
    
    _logger.debug("We want to delete: ")
    _logger.debug(sorted_snaps)
    
    for snap in sorted_snaps:
        try:
            command = f'rbd snap rm {_location}@{snap}'
            subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
        except OSError:
            _logger.error("Deletion of snapshot %s failed" % snap)
        except ValueError as ve:
            _logger.error(ve)
        else:
            _logger.debug("Successfully deleted snapshot %s " % snap)

def rbd_mount_newest_snapshot(_location, _prefix, _mount_location, _logger):
    _logger.info("rbd_mount_newest_snapshot: Mounting snapshots")

    snaps = rbd_list_snapshots(_location, _prefix)
    newest_snapshot = sorted(snaps, reverse = True)[0]
    
    result = rbd_unmount_snapshot(_location, newest_snapshot, _mount_location)
    if result:
        result += rbd_mount_snapshot(_location, newest_snapshot, _mount_location)
    else:
        _logger.warn(f"rbd_mount_newest_snapshot: Lock on {_mount_location}")
    
    if result == 1:
        _logger.warn(f"rbd_mount_newest_snapshot: Mounting failed for {_mount_location}")
    
    return result

def rbd_mount_snapshot(_location, _newest_snapshot, _mount_location):
    
    lock_file = _mount_location + ".lock"
    if os.path.exists(lock_file):
        return False
    
    ## protect snap
    command = f'rbd snap protect {_location}@{_newest_snapshot}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## clone snap
    command = f'rbd clone {_location}@{_newest_snapshot} {_location}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## map clone
    command = f'rbd map {_location}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).stdout.read()
    newest_snap_device = newest_snap_cmd.decode("utf-8").strip()
    
    ## mount clone
    os.makedirs(_mount_location, exist_ok = True)
    command = f'mount {newest_snap_device} {_mount_location}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    return True


def rbd_unmount_snapshot(_location, _newest_snapshot, _mount_location):
    
    lock_file = _mount_location + ".lock"
    if os.path.exists(lock_file):
        return False
    
    ## unmount clone
    command = f'umount {_mount_location}'
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

    return True