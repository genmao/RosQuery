#!/usr/bin/env python
# license removed for brevity
import rospy
from std_msgs.msg import Float64MultiArray
from std_msgs.msg import MultiArrayDimension


def talker():
    pub = rospy.Publisher('pcd_query_msg', Float64MultiArray, queue_size=1)
    rospy.init_node('talker', anonymous=True)
    rate = rospy.Rate(0.5)  # 0.5hz
    data = Float64MultiArray()

    while not rospy.is_shutdown():
        data.data = [32, -117]
        pub.publish(data)
        rate.sleep()

if __name__ == '__main__':
    try:
        talker()
    except rospy.ROSInterruptException:
        pass
