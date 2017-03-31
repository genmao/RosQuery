#!/usr/bin/env python

from pymongo import MongoClient
from PcdQuery_new import pcd_query
import rospy
import numpy as np
from std_msgs.msg import Float64MultiArray

pub = None
sub = None
point_list = None


def find_nearby_points(data):
    print "Received coordinate"
    client = MongoClient()
    db = client.block
    lon = np.float64(data.data[1])
    lat = np.float64(data.data[0])
    area_size = 100
    point_list = pcd_query(lon, lat, db, area_size)
    mat = Float64MultiArray()
    mat.data = point_list
    pub.publish(mat)
    print len(mat.data)
    # print mat.data


def pcd_query_server():
    global pub, point_list
    # Listen
    rospy.init_node('pcd_query_server', anonymous=True)
    pub = rospy.Publisher('surrounding_pcd_points', Float64MultiArray, queue_size=1)
    rospy.Subscriber('pcd_query_msg', Float64MultiArray, find_nearby_points)
    print "Query finished. \n"
    rospy.spin()

if __name__ == '__main__':
    try:
        pcd_query_server()
    except rospy.ROSInterruptException:
        pass
