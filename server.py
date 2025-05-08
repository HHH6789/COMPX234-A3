# 导入socket,threading,time模块
#Import the socket,threading,time module.
import socket
import threading
import time
#Import defaultdict from collections to create dictionaries with default values
from collections import defaultdict
# Import datetime and timedelta from datetime for date and time calculations
from datetime import datetime, timedelta

# 定义TupleSpaceServer类
# Define the TupleSpaceServer class.
class TupleSpaceServer:
    # 初始化方法
    # Initialization method, sets the server port and initializes data structures
    def __init__(self, port):