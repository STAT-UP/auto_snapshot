import os
import re
import datetime
import subprocess

def cephfs_create_snapshot(_location, _prefix, _logger):
    _logger.info("cephfs_create_snapshot: Creating snapshot")
    
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    
    try:
        if not(os.path.exists(_location)):
            raise ValueError("Directory " + _location + " does not exist")
        path = os.path.join(_location, ".snap", _prefix + "_-_" + now)
        os.mkdir(path)
    except OSError:
        _logger.error("Creation of the directory %s failed" % path)
    except ValueError as ve:
        _logger.error(ve)
    else:
        _logger.debug("Successfully created the directory %s " % path)


def cephfs_list_snapshots(_location, _prefix):
    path = os.path.join(_location, ".snap")
    snapdirs = [f 
                for f in os.listdir(path) 
                if os.path.isdir(os.path.join(path, f)) 
                    and re.match(_prefix, f)]
    return snapdirs


def cephfs_delete_old_snapshots(_location, _prefix, _n_retain, _logger):
    _logger.info("cephfs_delete_old_snapshots: Deleting snapshots")
    
    snaps = cephfs_list_snapshots(_location, _prefix)
    
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
            path = os.path.join(_location, ".snap", snap)
            if not(os.path.exists(path)):
                raise ValueError("Snapshot " + path + " does not exist")
            subprocess.Popen(f'rmdir {path}', shell = True, stdout = subprocess.PIPE).wait()
        except OSError:
            _logger.error("Deletion of snapshot %s failed" % path)
        except ValueError as ve:
            _logger.error(ve)
        else:
            _logger.debug("Successfully deleted snapshot %s " % path)


def cephfs_mount_newest_snapshot(_location, _prefix, _mount_location, _logger):
    _logger.info("cephfs_mount_newest_snapshot: Mounting snapshots")
    
    snaps = cephfs_list_snapshots(_location, _prefix)
    newest_snapshot = sorted(snaps, reverse = True)[0]
    snapshot_path = os.path.join(_location, ".snap", newest_snapshot)
    
    result = cephfs_unmount_snapshot(snapshot_path, _mount_location)
    if result:
        result += cephfs_mount_snapshot(snapshot_path, _mount_location)
    else:
        _logger.warn(f"cephfs_mount_newest_snapshot: Lock on {_mount_location}")
    
    if result == 1:
        _logger.warn(f"cephfs_mount_newest_snapshot: Mounting failed for {_mount_location}")

    return result

def cephfs_mount_snapshot(_snapshot_path, _mount_location):
    lock_file = _mount_location + ".lock"
    
    if os.path.exists(lock_file):
        return False
    
    os.symlink(_snapshot_path, _mount_location)
    
    return True

def cephfs_unmount_snapshot(_snapshot_path, _mount_location):
    lock_file = _mount_location + ".lock"
    
    if os.path.exists(lock_file):
        return False
    
    os.remove(_mount_location)
    
    return True
