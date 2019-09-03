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
    _logger.debug("rbd_delete_old_snapshots started")    
    snaps = rbd_list_snapshots(_location, _prefix)
    
    _logger.debug("All snapshots: ")
    _logger.debug(snaps)
    
    sorted_snaps = sorted(snaps, reverse = True)
    if _n_retain > 0:
        keep = max(0, _n_retain - 1)
        del sorted_snaps[:keep]
    
    _logger.debug("We want to delete: ")
    _logger.debug(sorted_snaps)
    
    _logger.info(f"rbd_delete_old_snapshots: Deleting {len(sorted_snaps)} snapshot(s)")
    
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
    pool = _location.split("/")[0]
    result = rbd_unmount_snapshot(_mount_location, pool, _logger = _logger)

    if result:
        _logger.info(f"rbd_mount_newest_snapshot: Mounting {_mount_location}")
        result += rbd_mount_snapshot(_location, _prefix, newest_snapshot, _mount_location, _logger)
    else:
        _logger.warn(f"rbd_mount_newest_snapshot: Lock on {_mount_location}")
    
    if result == 1:
        _logger.warn(f"rbd_mount_newest_snapshot: Mounting failed for {_mount_location}")
    
    return result


def rbd_mount_snapshot(_location, _prefix, _snapshot, _mount_location, _logger):
    
    lock_file = _mount_location + ".lock"
    if os.path.exists(lock_file):
        return False
    
    ## protect snap
    _logger.debug(f'[Mount] Protecting {_location}@{_snapshot}')
    command = f'rbd snap protect {_location}@{_snapshot}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## clone snap
    _logger.debug(f'[Mount] Cloning {_location}@{_snapshot} into {_location}_{_prefix}_backup')
    command = f'rbd clone {_location}@{_snapshot} {_location}_{_prefix}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## map clone
    _logger.debug(f'[Mount] Mapping {_location}v_backup')
    command = f'rbd map {_location}_{_prefix}_backup'
    newest_snap_cmd = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).stdout.read()
    newest_snap_device = newest_snap_cmd.decode("utf-8").strip()
    
    ## mount clone
    _logger.debug(f'[Mount] Mounting {_mount_location}')

    _logger.debug(f'[Mount] Check filesystem type')
    
    command = f'blkid {newest_snap_device}'
    newest_snap_fs_string = \
        subprocess.Popen(command, 
                         shell = True, 
                         stdout = subprocess.PIPE) \
        .stdout \
        .read() \
        .decode("utf-8") \
        .strip()
    newest_snap_fs = re.search(fr'TYPE="([^"]+)"', newest_snap_fs_string).group(1)
    _logger.debug(f'[Mount] Filesystem blkid: {newest_snap_fs_string}; type: {newest_snap_fs}')

    if newest_snap_fs == "xfs":
        mount_options = "-o nouuid"
    else:
        mount_options = ""
    
    os.makedirs(_mount_location, exist_ok = True)
    command = f'mount {mount_options} {newest_snap_device} {_mount_location}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    return True


def rbd_get_mount_info(_mount_location, _logger, _pool = "replicapool"):
    
    _logger.debug(f'[Unmount][mount_info] Find device of {_mount_location}')

    our_dev = None

    mounted_devs = subprocess.Popen(f'mount', 
                                    shell = True, 
                                    stdout = subprocess.PIPE) \
                        .stdout \
                        .read() \
                        .decode("utf-8") \
                        .split("\n")
    our_dev_string = [f 
                      for f in mounted_devs
                      if re.search(fr'\s{_mount_location}\s', f)]
    
    if len(our_dev_string) == 0:
        return "Not mounted"

    if len(our_dev_string) > 1:
        raise Exception(f"{_mount_location} should appear exactly once in mount output")
    
    our_dev = re.search(fr'(/dev/[^\s]+)', our_dev_string[0]).group(0)
    _logger.debug(f'[Unmount][mount_info] our device: {our_dev}')
    
    showmapped_output = \
        subprocess \
            .Popen(f'rbd showmapped', 
                shell = True, 
                stdout = subprocess.PIPE) \
            .stdout \
            .read() \
            .decode("utf-8") \
            .split("\n")
    our_showmapped_string = [f 
                            for f in showmapped_output
                            if re.search(fr'{our_dev}[^0-9]', f)]

    print(our_showmapped_string)
    
    if len(our_showmapped_string) != 1:
        raise Exception(f"{our_dev} should appear exactly once in rbd showmapped")
    
    our_image = re.search(fr'([\S]+)\s+[\S]+\s+{our_dev}', our_showmapped_string[0]).group(1)
    _logger.debug(f'[Unmount][mount_info] Our image: {our_image}')

    our_ls_output = \
        subprocess \
            .Popen(f'rbd ls -l {_pool}', 
                    shell = True, 
                    stdout = subprocess.PIPE) \
            .stdout \
            .read() \
            .decode("utf-8") \
            .split("\n")

    our_ls_string = [f 
                    for f in our_ls_output
                    if re.search(fr'^{our_image}', f)]
    
    if len(our_ls_string) != 1:
        raise Exception(f"{our_image} should appear exactly once in rbd ls -l. Instead we get {our_ls_string}")
    
    our_parent = re.split(r'\s+', our_ls_string[0])[3]
        
    return {"mount_location": _mount_location,
            "device": our_dev, 
            "image": our_image, 
            "pool": _pool, 
            "parent": our_parent}


def rbd_unmount_snapshot(_mount_location, _pool, _logger):
    info = rbd_get_mount_info(_mount_location, _logger, _pool = _pool)

    if info == "Not mounted":
        _logger.info(f"{_mount_location} not mounted")
        return True

    lock_file = _mount_location + ".lock"
    if os.path.exists(lock_file):
        _logger.info(f"{_mount_location} is locked. If this is an error remove {_mount_location}.lock")
        return False
    
    ## unmount clone
    _logger.debug(f'[Unmount] Unmounting {_mount_location}')
    command = f'umount {_mount_location}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## unmap clone
    _logger.debug(f'[Unmount] Unmapping {info["image"]}')
    more_than_once = True
    attempt = 1

    while bool(more_than_once) and attempt <= 5:
        command = f'rbd unmap {info["pool"]}/{info["image"]}'
        result = \
            subprocess \
                .Popen(command, 
                        shell = True, 
                        stdout = subprocess.PIPE) \
                .stdout \
                .read() \
                .decode("utf-8")
        _logger.debug(f'[Unmount] Ran unmount with result {result}')
        attempt += 1
        more_than_once = re.search(fr'mapped more than once', result)

    ## delete clone
    _logger.debug(f'[Unmount] Deleting {info["image"]}')
    command = f'rbd rm {info["pool"]}/{info["image"]}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()
    
    ## unprotect snap
    _logger.debug(f'[Unmount] Unprotecting {info["parent"]}')
    command = f'rbd snap unprotect {info["parent"]}'
    subprocess.Popen(command, shell = True, stdout = subprocess.PIPE).wait()

    return True