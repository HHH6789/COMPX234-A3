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

            # 检查请求格式是否有效（至少3部分）
            # Check if the request format is valid (at least 3 parts)
            if len(parts) < 3:
                return self.format_error("Invalid request format")
            
            # 获取请求大小
            # Get the request size
            size = int(parts[0])

            # 获取操作类型并转换为大写
            # Get the operation type and convert to uppercase
            operation = parts[1].upper()

            # 获取键（对于PUT操作，键是第2部分到倒数第二部分）
            # Get the key (for PUT operation, key is from part 2 to the second last part)
            key = ' '.join(parts[2:-1]) if operation == 'P' else ' '.join(parts[2:])

            # 如果是PUT操作，获取值（最后一部分）
            # If it's a PUT operation, get the value (last part)
            if operation == 'P':
                value = parts[-1]
            else:
                value = None
            
            # 验证请求大小是否匹配
            # Validate if the request size matches
            expected_size = 3 + len(operation) + len(key) + (len(value) + 1 if value else 0)
            if expected_size != size:
                return self.format_error("Size mismatch in request")
            

            # 根据操作类型处理请求
            # Process the request based on the operation type
            # Check if the operation is 'R' (Read)
            # 检查操作是否为'R'（读取）
            if operation == 'R':
                # Call process_read method with the key and return its result
                # 调用process_read方法处理key并返回结果
                return self.process_read(key)
            # If not 'R', check if operation is 'G' (Get)
            # 如果不是'R'，检查操作是否为'G'（获取）
            elif operation == 'G':
                # Call process_get method with the key and return its result
                # 调用process_get方法处理key并返回结果
                return self.process_get(key)
            # If not 'R' or 'G', check if operation is 'P' (Put)
            # 如果不是'R'或'G'，检查操作是否为'P'（存放）
            elif operation == 'P':
                # Call process_put method with both key and value, return its result
                # 调用process_put方法处理key和value并返回结果
                return self.process_put(key, value)
            # If operation is none of the above
            # 如果操作不是以上任何一种
            else:
                # Return a formatted error message for invalid operation
                # 返回格式化的错误信息表示无效操作
                return self.format_error("Invalid operation")
        
        # 处理异常并返回错误信息
        # Handle exceptions and return error message
        except Exception as e:
            return self.format_error(str(e))
        

    # 定义处理READ操作的方法
    # define method to handle READ operations
    def process_read(self, key):
        # 使用锁确保线程安全
        # Use lock to ensure thread safety
        with self.lock:
            # 更新操作统计
            # Update operation statistics
            self.stats['total_operations'] += 1
            self.stats['total_reads'] += 1

            # 检查键是否存在
            # Check if the key exists
            if key in self.tuple_space:
                # 获取值并返回成功响应
                # Get the value and return success response
                value = self.tuple_space[key]
                return self.format_response(f"OK ({key}, {value}) read")
            # 键不存在，返回错误并更新错误统计
            # Key doesn't exist, return error and update error statistics
            else:

                self.stats['total_errors'] += 1
                return self.format_response(f"ERR {key} does not exist")


    # 定义处理GET操作的方法
    # define method to handle GET operations
    def process_get(self, key):
        # 使用锁确保线程安全
        # Use lock to ensure thread safety
        with self.lock:

