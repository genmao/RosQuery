import numpy as np
import pypcd
from pymongo import MongoClient, GEO2D
from pymongo.errors import BulkWriteError
from pprint import pprint
from collections import defaultdict
import datetime


# parse_gps_txt(f) reads GPS and Pose data in file f (*.txt) to gps_array and pose_array
# gps_array: [longitude, latitude, altitude]
# pose_array: [x, y, z]
def parse_gps_txt(f):
    gps_list = []
    pose_list = []
    for line in open(f):
        fields = line.split(';')
        tmp1 = fields[2].split(':')[1].split(',')
        gps_list.append(map(float, tmp1))
        tmp2 = fields[1].split(':')[1].split(',')
        pose_list.append(map(float, tmp2))
    gps_array = np.asarray(gps_list)
    pose_array = np.asarray(pose_list)
    return gps_array, pose_array


# load_global_map(f, db, drop) reads pcd data in f (*.pcd) into Mongodb collection db.block
# The format is as {"loc": [x, z], "y": [y]}
# By default, when drop==False, it would append new data without remove old points.
# To remove old data in db.offline, please set drop=True
def load_global_map(f, db, drop=False):
    print "Reading pcd...\n"
    start = datetime.datetime.now()
    print start
    pc = pypcd.PointCloud.from_path(f)
    global_map = pc.pc_data  # global_map is a numpy array
    print datetime.datetime.now() - start

    print "Dumping into dictionary...\n"
    block_size = 1  # set block size as 1*1
    # points_to block serves as a dictionary <block: points>.
    point_to_block = defaultdict(list)
    for point in global_map:
        x_block = np.floor(point[0]/block_size)*block_size
        z_block = np.floor(point[2]/block_size)*block_size
        block_tuple = (x_block, z_block)
        point_to_block.setdefault(block_tuple, []).append([np.float64(point[0]), np.float64(point[2]), np.float64(point[1])])
    print datetime.datetime.now() - start

    # CAUTION: This 'if' will remove all old points in db.block.
    if drop:
        print "Dropping old data in offlinemap...\n"
        db.block.drop()
        db.block.create_index([("loc", GEO2D)], min=-1e7, max=1e7)
        print "Old data dropped. \n"

    print "Adding new data...\n"

    from signal import signal, SIGPIPE, SIG_DFL
    signal(SIGPIPE, SIG_DFL)

    # Using unordered bulk insert to accelerate
    bulk = db.block.initialize_unordered_bulk_op()
    length = len(point_to_block)
    batch_start = 0
    batch_size = int(1e7)
    batch_end = min(batch_start + batch_size, length)
    for block in point_to_block:
        points = point_to_block[block]
        bulk.insert({"loc": block, "points": points})
        batch_start += 1
        if batch_start == batch_end:
            try:
                bulk.execute()
            except BulkWriteError as bwe:
                pprint(bwe.details)
            print "%d data added. \n" % batch_end
            bulk = db.block.initialize_unordered_bulk_op()
            batch_start = batch_end
            batch_end = min(batch_start + batch_size, length)

    print "All new data added. \n"
    print datetime.datetime.now() - start
    return


# load_gps_map(f, db, drop) reads GPS and Pose data in f (*.pcd) into Mongodb collection db.locmap
# The format is as {"loc": [longitude, latitude], "Height": Altitude, "x":x, "y":y, "z":z}
# By default, when drop==False, it would append new data without remove old points.
# To remove old data in db.offline, please set drop=True
def load_gps_map(f, db, drop=True):
    gps_list, pose_list = parse_gps_txt(f)
    if len(gps_list) != len(pose_list):
        print "Error: GPS and Pose size do not match. \n"
        return
    if drop:
        print "Dropping old data in offlinemap...\n"
        db.locmap.drop()
        db.locmap.create_index([("loc", GEO2D)])
        print "Old data dropped. \n"
    length = len(gps_list)
    batch_start = 0
    batch_size = int(1.5e7)
    batch_end = min(batch_start + batch_size, length)
    print "Adding new data...\n"
    while batch_start < length:
        bulk = db.locmap.initialize_unordered_bulk_op()
        for (gps, pose) in zip(gps_list[batch_start: batch_end], pose_list[batch_start: batch_end]):

            bulk.insert(
                {
                    "loc": [np.float64(gps[1]), np.float64(gps[0])],  # [longitude, latitude]
                    "Height": np.float64(gps[2]),  # altitude
                    "x": np.float64(pose[3]),
                    "y": np.float64(pose[4]),
                    "z": np.float64(pose[5])
                }
            )
        try:
            bulk.execute()
        except BulkWriteError as bwe:
            pprint(bwe.details)
        print "%d data added. \n" % batch_end
        batch_start += batch_size
        batch_end = min(batch_start + batch_size, length)
    print "All new data added. \n"
    return


def pcd_loader(txtfile, pcdfile, db, drop_loc=False, drop_global=False):
    # Read txt data into "locmap"
    load_gps_map(txtfile, db, drop_loc)
    # Read pcd data into "offlinemap"
    load_global_map(pcdfile, db, drop_global)
    return

if __name__ == "__main__":
    client = MongoClient()
    db = client.block
    txtfile = '/home/genmaoshi/Downloads/data_sample/1486175959.4.txt'
    pcdfile = '/home/genmaoshi/Downloads/data_sample/1486176044.29.pcd'
    # pcdfile = '/home/genmaoshi/Downloads/data_sample/part.pcd'
    pcd_loader(txtfile, pcdfile, db, drop_loc=True, drop_global=True)
