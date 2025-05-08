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
        # 服务器端口号
        # Server port number
        self.port = port

        # 元组空间，用字典存储键值对
        # Tuple space, using a dictionary to store key-value pairs
        self.tuple_space = {}

        # 线程锁，用于线程同步
        # Thread lock for thread synchronization
        self.lock = threading.Lock()

        # 统计信息字典，记录服务器运行状态
        # Statistics dictionary to record server operation status
        self.stats = {
            # 总客户端数
            # Total number of clients
            'total_clients': 0,
            # 总操作数
            # Total number of operations
            'total_operations': 0,
            # 总读取操作数
            # Total number of read operations
            'total_reads': 0,
            # 总获取操作数
            # Total number of get operations
            'total_gets': 0,
            # 总放置操作数
            # Total number of put operations
            'total_puts': 0, 
            # 总错误数
            # Total number of errors
            'total_errors': 0,
        }

        # 记录上次报告统计信息的时间
        # Record the time of the last statistics report
        self.last_report_time = datetime.now()
    
    # 启动服务器
    # start the server
    def start(self):
        # 创建TCP socket
        # Create a TCP socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)