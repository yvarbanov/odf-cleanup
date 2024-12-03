#!/bin/python

"""
name:             :odfcleanup.py
description       :deletes odf objects based on a given ID/LAB
author            :yvarbev@redhat.com
version           :24.12.02
"""

import rbd
import time
import os


def ioctx_get():
    """
    Connect to an odf cluster and a given pool.
    Returns:  ioctx   (rados.Ioctx)  - rbd context
    """
    import rados
    POOL = os.environ['CL_POOL']
    CONF = os.environ['CL_CONF']
    KEYRING = os.environ['CL_KEYRING']
    cluster = rados.Rados(conffile=CONF, conf=dict(keyring=KEYRING))
    print(f"\nioctx_get> librados version: {cluster.version()}")
    print(f"ioctx_get> Will attempt to connect to: {cluster.conf_get('mon host')}")
    cluster.connect()
    print(f"ioctx_get> Cluster ID: {cluster.get_fsid()}\n")
    return cluster.open_ioctx(POOL)


def rm_image(ioctx, obj):
    """
    Delete an image from a ceph cluster, including its snapshots
    and unprotect them if they are protected.
    Parameters: ioctx   (rados.Ioctx)  - rbd context
                obj     (str)          - ceph image name
    Notes: - csi-snap can be a descendant but not a snap of its parent
           - the evaluate func should have filtered images ready for deletion
    Returns: xrm        (bol)          - False if image was removed correctly
    """
    print("rm_image> Attempting to delete image: ", obj)
    xrm = True
    try:
        img = rbd.Image(ioctx, obj)
        # get number of descendants if any
        dcharge = sum(1 for d in img.list_descendants())
        if dcharge == 0:
            print("rm_image 1> no descendants found")
            try:
                # try to flatten the image
                img.flatten()
                print("rm_image> image flattened")
                # if flatten have to wait for a bit for the change
                time.sleep(15)
            except:
                pass
            # csi-snaps can have snaps even if they have no descendants:
            scharge = sum(1 for s in img.list_snaps())
            if scharge == 0:
                # there are items to remove but no snaps, in this case the image should not have been selected for removal
                print("rm_image> No snaps to remove")
            else:
                # clean the snaps and remove the image
                for snap in img.list_snaps():
                    print("rm_image> Removing snapshot", snap['name'])
                    # If image is protected, unprotect it
                    if img.is_protected_snap(snap['name']):
                        img.unprotect_snap(snap['name'])
                    img.remove_snap(snap['name'])
                    time.sleep(5)
            # now delete the image
            img.close()
            time.sleep(5)
            try:
                print("rm_image> Removing ", obj)
                rbd.RBD().remove(ioctx, obj)
            except Exception as e:
                xrm = False
                print(f"rm_image> {e}")
        else:
            # check if there are any descendants that are not in the trash
            tcharge = sum(1 for t in img.list_descendants() if not t['trash'])
            if tcharge == 0:
                # all the related images are already in the trash
                # csi-snaps can have snaps even if they have no descendants:
                scharge = sum(1 for s in img.list_snaps())
                if scharge == 0:
                    # there are items to remove but no snaps, in this case the image should not have been selected for removal
                    print("rm_image> No snaps to remove")
                else:
                    # clean the snaps and remove the image
                    for snap in img.list_snaps():
                        print("rm_image> Removing snapshot", snap['name'])
                        # If image is protected, unprotect it
                        if img.is_protected_snap(snap['name']):
                            img.unprotect_snap(snap['name'])
                        img.remove_snap(snap['name'])
                        time.sleep(5)
                img.close()
                time.sleep(5)
                try:
                    print("rm_image> Removing ", obj)
                    rbd.RBD().remove(ioctx, obj)
                except Exception as e:
                    xrm = False
                    print(f"rm_image> {e}")
            else:  
                # since there are descendants not in the trash, check to see if any are snapshots
                scharge = sum(1 for s in img.list_snaps())
                if scharge == 0:
                    # there are items to remove but no snaps, in this case the image should not have been selected for removal
                    print("rm_image> No snaps to remove")
                    raise Exception("rm_image> remove the pending images first.")
                else:
                    # clean the snaps and remove the image
                    for snap in img.list_snaps():
                        print("rm_image> Removing snapshot", snap['name'])
                        # If image is protected, unprotect it
                        if img.is_protected_snap(snap['name']):
                            img.unprotect_snap(snap['name'])
                        img.remove_snap(snap['name'])
                        time.sleep(5)
                    # now delete the image
                    img.close()
                    time.sleep(5)
                    try:
                        print("rm_image> Removing ", obj)
                        rbd.RBD().remove(ioctx, obj)
                    except Exception as e:
                        xrm = False
                        print(f"rm_image> {e}")
    except Exception as e:
        print("rm_image>" ,e)
    finally:
        img.close()
    return xrm


def evaluate(ioctx, lst):
    """
    Evaluates which image has the lowest number of descendants.
    The image with the lowest number (pref 0) should be selected for removal.
    Parameters: ioctx   (rados.Ioctx)  - rbd context
                lst     (list)         - list with odf images (names)
    Returns:  tuple ((image position in list), (number of descendants))
    """
    values = []
    for img in lst:
        try:
            image = rbd.Image(ioctx, img)
            charge = sum(1 for n in image.list_descendants())
            print(f"evaluate> {image}, {charge}")
            values.append(charge)
        except:
            # probably no parent
            pass
        finally:
            image.close()
    return values.index(min(values)), min(values)


def list_gen(ioctx):
    """
    Generates a list of odf images based on a value.
    It also checks if any csi-snap parent/child has said value.
    Parameters: ioctx   (rados.Ioctx)  - rbd context
    Returns:  list of images that matched the LAB env
    """
    LAB = os.environ['CL_LAB']
    lab_vols = [x for x in rbd.RBD().list(ioctx) if LAB in x]
    lab_csi = []
    lab_vols_trash = [x for x in rbd.RBD().trash_list(ioctx) if LAB in x['name']]
    all_csi_snaps = [x for x in rbd.RBD().list(ioctx) if 'csi-snap' in x]
    for csi in all_csi_snaps:
        try:
            image = rbd.Image(ioctx, csi)
            if LAB in image.parent_info()[1]:
                lab_csi.append(csi)
        except:
            # probably no parent
            pass
        finally:
            image.close()
    to_remove = lab_vols + lab_csi
    return to_remove


def main():
    if 'CL_LAB' in os.environ and 'CL_POOL' in os.environ:
        print("main>", os.environ['CL_LAB'], os.environ['CL_POOL'])
    else:
        raise Exception('main> LAB and POOL envs missing')
    if 'CL_CONF' in os.environ and 'CL_KEYRING' in os.environ:
        ioctx = ioctx_get()
    else:
        raise Exception('main> CONF and KEYRING envs missing')
    print("main> Generate list to remove")
    to_remove = list_gen(ioctx)
    while to_remove:
        print("main> Pick object to delete")
        ev, mn = evaluate(ioctx, to_remove)
        img = to_remove[ev]
        if mn == 0:
            print(f"Found image with no descendants: {img}")
        else:
            print(f"Lowest number of descendants: {mn} for {img}")
        try:
            # if false keep the img in the list
            if rm_image(ioctx, img):
                print("main> Remove image from list")
                to_remove.remove(img)
            time.sleep(20)
        except Exception as e:
            print(e)
            raise Exception(f"main> Something went wrong with {img}")
    if ioctx:
        ioctx.close()


if __name__ == "__main__":
    main()
