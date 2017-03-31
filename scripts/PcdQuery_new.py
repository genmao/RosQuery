import numpy as np
from pymongo import MongoClient
import datetime
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


def pcd_query(lon, lat, db, query_size=10000):
    # Query GPS
    start = datetime.datetime.now()
    cursor2 = db.locmap.find(
        {
            "loc": {
                "$near": [lon, lat]  # query_gps
            }
        }
    ).limit(1)
    query_res = []
    for l in cursor2:
        query_res.append([l['x'], l['z']])
        print 'Current coordinates: ' + 'x=' + str(l['x']) + ' z=' + str(l['z'])

    # Query pdc data
    cur_x = query_res[0][0]
    cur_z = query_res[0][1]
    cursor = db.block.find(
        {
            "loc": {
                "$near": [cur_x, cur_z]
            }
        }
    ).limit(int(query_size))
    print datetime.datetime.now() - start
    print "Cursor Ready!\n"

    points = []
    for l in cursor:
        points.extend(l["points"])
    # print points
    print datetime.datetime.now() - start
    print "List Ready!\n"
    '''
    points_array = np.asarray(points)
    # Visualization
    X = points_array[:, 0]
    Y = points_array[:, 2]
    Z = points_array[:, 1]
    # points = np.asarray(points)
    print datetime.datetime.now() - start
    print "Array Ready!\n"
    return X, Y, Z
    '''
    return points

if __name__ == "__main__":
    client = MongoClient()
    db = client.block
    query_gps = [32.8687, -117.228551095]
    lon = np.float64(query_gps[1])
    lat = np.float64(query_gps[0])
    query_size = 1000
    points = pcd_query(lon, lat, db, query_size)
    print len(points)
    '''
    X, Y, Z = pcd_query(lon, lat, db, query_size)
    print len(X)
    # Visualization
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(X, Z, Y)
    ax.set_xlabel('X')
    ax.set_ylabel('Z')
    ax.set_zlabel('Y')
    plt.show()
    '''
