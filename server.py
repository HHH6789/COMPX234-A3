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

        # 绑定服务器地址和端口
        # Bind the server address and port
        server_socket.bind(('0.0.0.0', self.port))

        # 开始监听，最大连接数为5
        # Start listening with a maximum of 5 connections
        server_socket.listen(5)

        # 打印服务器启动信息
        # Print server startup information
        print(f"Server started on port {self.port}")

        # 创建并启动统计信息报告线程
        # Create and start the statistics reporting thread
        stats_thread = threading.Thread(target=self.report_stats_periodically, daemon=True)
        stats_thread.start()


        # 主循环，接受客户端连接
        # Main loop to accept client connections
        try:
            while True:
                # 接受客户端连接
                # Accept a client connection
                client_socket, addr = server_socket.accept()

                # 客户端数加1
                # Increment the client count
                self.stats['total_clients'] += 1

                # 打印新客户端连接信息
                # Print new client connection information
                print(f"New client connected: {addr}")

                # 创建并启动处理客户端请求的线程
                # Create and start a thread to handle client requests
                client_thread = threading.Thread(
                    # Execute handle_cient method
                    # 执行handle_client方法
                    target=self.handle_client,
                    #以client_socket作为参数
                    #Using the parameter 'client-side socket'
                    args=(client_socket,)

                )
                # Start the newly created thread
                # 启动新创建的线程
                client_thread.start()
        
        # 键盘中断，关闭服务器
        # Catch keyboard interrupt to shut down the server
        except KeyboardInterrupt: 
            print("\nServer shutting down...")
        # 关闭服务器socket
        # Close the server socket
        finally: 
            server_socket.close()
    
    # 定义定期报告统计信息的方法
    # define method to periodically report statistics
    def report_stats_periodically(self):
        #When the conditions are met
        while True:
            # 每10秒报告一次
            # Report every 10 seconds
            time.sleep(10)
            self.report_stats()

    # 定义方法来使报告统计信息的具体实现
    # Defining methods to achieve specific implementation of statistical information in reports
    def report_stats(self):
        # 使用锁确保线程安全
        # Use lock to ensure thread safety
        with self.lock:
            # 获取当前时间
            # Get current time
            current_time = datetime.now()


            # 检查是否达到报告间隔（10秒）
            # Check if the reporting interval (10 seconds) has been reached
            if current_time - self.last_report_time >= timedelta(seconds=10):
                # 计算元组数量
                # Calculate the number of tuples
                num_tuples = len(self.tuple_space)

                # 计算所有键的总长度
                # Calculate the total length of all keys
                total_key_size = sum(len(k) for k in self.tuple_space.keys())

                # 计算所有值的总长度
                # Calculate the total length of all values
                total_value_size = sum(len(v) for v in self.tuple_space.values())

                # 计算平均键长度
                # Calculate the average key length
                avg_key_size = total_key_size / num_tuples if num_tuples > 0 else 0

                # 计算平均值长度
                # Calculate the average value length
                avg_value_size = total_value_size / num_tuples if num_tuples > 0 else 0

                # 计算平均元组长度
                # Calculate the average tuple length
                avg_tuple_size = (total_key_size + total_value_size) / num_tuples if num_tuples > 0 else 0

                # 打印统计信息
                # Print statistics
                print("\n=== Server Statistics ===")
                print(f"Tuples: {num_tuples}")
                print(f"Avg tuple size: {avg_tuple_size:.2f} chars")
                print(f"Avg key size: {avg_key_size:.2f} chars")
                print(f"Avg value size: {avg_value_size:.2f} chars")
                print(f"Total clients: {self.stats['total_clients']}")
                print(f"Total operations: {self.stats['total_operations']}")
                print(f"  READs: {self.stats['total_reads']}")
                print(f"  GETs: {self.stats['total_gets']}")
                print(f"  PUTs: {self.stats['total_puts']}")
                print(f"  Errors: {self.stats['total_errors']}")
                print("=======================\n")

                # 更新上次报告时间
                # Update the last report time
                self.last_report_time = current_time


    # 定义处理客户端连接的方法
    # define method to handle client connections
    def handle_client(self, client_socket):
        try:
            #When the conditions are met
            while True:
                # 接收客户端数据（最大1024字节）
                # Receive client data (max 1024 bytes)
                data = client_socket.recv(1024).decode('utf-8')

                # 如果没有数据，断开连接
                # If no data, disconnect
                if not data:
                    break

                # 处理请求并获取响应
                # Process the request and get the response
                response = self.process_request(data)

                # 发送响应给客户端
                # Send the response to the client
                client_socket.send(response.encode('utf-8'))
        
        
        # 处理客户端异常
        # Handle client exceptions
        except ConnectionResetError:
            print("Client disconnected unexpectedly")
        finally:
            # 关闭客户端socket
            # Close the client socket
            client_socket.close()
    

    # 定义处理客户端请求的方法
    # define method to process client requests
    def process_request(self, request):
        # 解析请求
        # Parse the request
        try:
            # 按空格分割请求
            # Split the request by spaces
            parts = request.split()




