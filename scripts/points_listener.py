#!/usr/bin/env python
# license removed for brevity
import rospy
from std_msgs.msg import Float64MultiArray


def callback(data):
    points = data.data
    print len(points)


def listener():
    rospy.init_node('points_listener', anonymous=True)
    rospy.Subscriber('surrounding_pcd_points', Float64MultiArray, callback)
    rospy.spin()

if __name__ == '__main__':
    try:
        listener()
    except rospy.ROSInterruptException:
        pass
