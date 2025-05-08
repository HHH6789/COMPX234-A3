# 导入socket模块用于网络通信 / Import socket module for network communication
import socket
# 导入threading模块用于多线程处理 / Import threading module for multi-threading
import threading
# 导入time模块用于时间相关操作 / Import time module for time-related operations
import time
# 从collections导入defaultdict用于创建默认字典 / Import defaultdict from collections for default dictionaries
from collections import defaultdict
# 从datetime导入datetime和timedelta用于日期时间计算 / Import datetime and timedelta for date/time calculations
from datetime import datetime, timedelta

# 定义TupleSpaceServer类 / Define TupleSpaceServer class
class TupleSpaceServer:
    # 初始化方法 / Initialization method
    def __init__(self, port):
        # 服务器端口号 / Server port number
        self.port = port
        # 元组存储空间字典 / Dictionary for tuple storage
        self.tuple_space = {}
        # 线程锁用于同步 / Thread lock for synchronization
        self.lock = threading.Lock()
        # 服务器统计信息字典 / Server statistics dictionary
        self.stats = {
            'total_clients': 0,      # 总客户端数 / Total clients
            'total_operations': 0,   # 总操作数 / Total operations
            'total_reads': 0,        # 读取操作数 / Read operations
            'total_gets': 0,         # 获取操作数 / Get operations
            'total_puts': 0,        # 存放操作数 / Put operations
            'total_errors': 0,       # 错误数 / Errors
        }
        # 上次报告统计信息的时间 / Last statistics report time
        self.last_report_time = datetime.now()
        
    # 启动服务器方法 / Server startup method
    def start(self):
        # 创建TCP socket / Create TCP socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 绑定服务器地址和端口 / Bind server address and port
        server_socket.bind(('0.0.0.0', self.port))
        # 开始监听，最大连接数5 / Start listening with max 5 connections
        server_socket.listen(5)
        # 打印服务器启动信息 / Print server startup message
        print(f"Server started on port {self.port}")
        
        # 启动统计信息报告线程 / Start statistics reporting thread
        stats_thread = threading.Thread(target=self.report_stats_periodically, daemon=True)
        stats_thread.start()
        
        try:
            # 主服务器循环 / Main server loop
            while True:
                # 接受客户端连接 / Accept client connection
                client_socket, addr = server_socket.accept()
                # 客户端计数增加 / Increment client count
                self.stats['total_clients'] += 1
                # 打印新客户端信息 / Print new client info
                print(f"New client connected: {addr}")
                
                # 创建客户端处理线程 / Create client handler thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                # 启动客户端线程 / Start client thread
                client_thread.start()
        # 捕获键盘中断 / Catch keyboard interrupt
        except KeyboardInterrupt:
            print("\nServer shutting down...")
        finally:
            # 关闭服务器socket / Close server socket
            server_socket.close()
    
    # 定期报告统计信息方法 / Periodic statistics reporting method
    def report_stats_periodically(self):
        while True:
            # 每10秒报告一次 / Report every 10 seconds
            time.sleep(10)
            # 调用报告方法 / Call report method
            self.report_stats()
    
    # 报告统计信息方法 / Statistics reporting method
    def report_stats(self):
        # 使用锁保证线程安全 / Use lock for thread safety
        with self.lock:
            # 获取当前时间 / Get current time
            current_time = datetime.now()
            # 检查是否达到报告间隔 / Check if reporting interval reached
            if current_time - self.last_report_time >= timedelta(seconds=10):
                # 计算元组数量 / Calculate tuple count
                num_tuples = len(self.tuple_space)
                # 计算所有键的总长度 / Calculate total key size
                total_key_size = sum(len(k) for k in self.tuple_space.keys())
                # 计算所有值的总长度 / Calculate total value size
                total_value_size = sum(len(v) for v in self.tuple_space.values())
                
                # 计算平均键长度 / Calculate average key size
                avg_key_size = total_key_size / num_tuples if num_tuples > 0 else 0
                # 计算平均值长度 / Calculate average value size
                avg_value_size = total_value_size / num_tuples if num_tuples > 0 else 0
                # 计算平均元组长度 / Calculate average tuple size
                avg_tuple_size = (total_key_size + total_value_size) / num_tuples if num_tuples > 0 else 0
                
                # 打印统计信息 / Print statistics
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
                
                # 更新最后报告时间 / Update last report time
                self.last_report_time = current_time
    
    # 处理客户端连接方法 / Client connection handler method
    def handle_client(self, client_socket):
        try:
            while True:
                # 接收客户端数据 / Receive client data
                data = client_socket.recv(1024).decode('utf-8')
                # 如果没有数据则断开 / Disconnect if no data
                if not data:
                    break
                    
                # 处理请求并获取响应 / Process request and get response
                response = self.process_request(data)
                # 发送响应给客户端 / Send response to client
                client_socket.send(response.encode('utf-8'))
        # 处理连接重置错误 / Handle connection reset error
        except ConnectionResetError:
            print("Client disconnected unexpectedly")
        finally:
            # 关闭客户端socket / Close client socket
            client_socket.close()
    
    # 处理请求方法 / Request processing method
    def process_request(self, request):
        try:
            # 清除请求首尾空白字符 / Strip whitespace from request
            request = request.strip()
        
            # === 基础验证 === / Basic validation
            # 检查最小有效请求长度 / Check minimum valid request length
            if len(request) < 5:  # 最小如 "005 R x" / Minimum like "005 R x"
               return self.format_error("Message too short")
        
            # === 提取长度前缀 === / Extract length prefix
            # 获取声明的长度字符串 / Get declared length string
            declared_length_str = request[:3]
            try:
               # 转换为整数 / Convert to integer
               declared_length = int(declared_length_str)
            except ValueError:
                return self.format_error("Invalid length prefix")
        
            # 验证声明的长度是否匹配实际长度 / Validate declared vs actual length
            actual_length = len(request)
            if actual_length != declared_length:
                return self.format_error(
                    f"Size mismatch (declared: {declared_length}, actual: {actual_length})"
                )
        
            # === 提取操作和参数 === / Extract operation and parameters
            # 跳过长度前缀和空格 / Skip length prefix and space
            remaining_msg = request[4:]
            # 分割消息（最多2次以兼容含空格的value） / Split message (max 2 splits for space-containing values)
            parts = remaining_msg.split(maxsplit=2)
        
            # 检查必要参数 / Check required parameters
            if len(parts) < 2 and parts[0] != 'P':
                return self.format_error("Missing key")
            
            # 获取操作类型（转为大写） / Get operation type (uppercase)
            operation = parts[0].upper()
            # 获取键 / Get key
            key = parts[1]
            # 如果是PUT操作则获取值 / Get value if PUT operation
            value = parts[2] if len(parts) > 2 and operation == 'P' else None
        
            # === 操作路由 === / Operation routing
            if operation == 'R':
                return self.process_read(key)
            elif operation == 'G':
                return self.process_get(key)
            elif operation == 'P':
                # PUT操作必须提供值 / PUT requires a value
                if value is None:
                    return self.format_error("PUT requires a value")
                return self.process_put(key, value)
            else:
                return self.format_error(f"Invalid operation: {operation}")
            
        # 捕获所有异常 / Catch all exceptions
        except Exception as e:
            return self.format_error(f"Internal error: {str(e)}")

    # 处理READ操作 / READ operation handler
    def process_read(self, key):
        # 使用锁保证线程安全 / Use lock for thread safety
        with self.lock:
            # 更新操作统计 / Update operation stats
            self.stats['total_operations'] += 1
            self.stats['total_reads'] += 1
            
            # 检查键是否存在 / Check if key exists
            if key in self.tuple_space:
                # 返回值 / Return value
                value = self.tuple_space[key]
                return self.format_response(f"OK ({key}, {value}) read")
            else:
                # 更新错误统计 / Update error stats
                self.stats['total_errors'] += 1
                return self.format_response(f"ERR {key} does not exist")
    
    # 处理GET操作 / GET operation handler
    def process_get(self, key):
        with self.lock:
            # 更新操作统计 / Update operation stats
            self.stats['total_operations'] += 1
            self.stats['total_gets'] += 1
            
            # 检查键是否存在 / Check if key exists
            if key in self.tuple_space:
                # 移除并返回值 / Remove and return value
                value = self.tuple_space.pop(key)
                return self.format_response(f"OK ({key}, {value}) removed")
            else:
                # 更新错误统计 / Update error stats
                self.stats['total_errors'] += 1
                return self.format_response(f"ERR {key} does not exist")
    
    # 处理PUT操作 / PUT operation handler
    def process_put(self, key, value):
        with self.lock:
            # 更新操作统计 / Update operation stats
            self.stats['total_operations'] += 1
            self.stats['total_puts'] += 1
            
            # 检查键是否已存在 / Check if key already exists
            if key in self.tuple_space:
                # 更新错误统计 / Update error stats
                self.stats['total_errors'] += 1
                return self.format_response(f"ERR {key} already exists")
            else:
                # 存储键值对 / Store key-value pair
                self.tuple_space[key] = value
                return self.format_response(f"OK ({key}, {value}) added")
    
    # 格式化响应方法 / Response formatting method
    def format_response(self, message):
        # 格式: NNN message (NNN是总长度) / Format: NNN message (NNN is total length)
        size = len(message) + 4  # 3位长度+1空格 / 3 for size + 1 space
        return f"{size:03d} {message}"
    
    # 格式化错误方法 / Error formatting method
    def format_error(self, error_msg):
        return self.format_response(f"ERR {error_msg}")

# 主函数 / Main function
def main():
    import sys
    # 检查命令行参数 / Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python server.py <port>")
        return
    
    try:
        # 获取端口号 / Get port number
        port = int(sys.argv[1])
        # 验证端口范围 / Validate port range
        if not (50000 <= port <= 59999):
            raise ValueError("Port must be between 50000 and 59999")
            
        # 创建并启动服务器 / Create and start server
        server = TupleSpaceServer(port)
        server.start()
    # 处理值错误 / Handle value error
    except ValueError as e:
        print(f"Error: {e}")
    # 处理其他异常 / Handle other exceptions
    except Exception as e:
        print(f"Server error: {e}")

# 程序入口 / Program entry point
if __name__ == "__main__":
    main()