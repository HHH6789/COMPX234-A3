# 导入socket和sys模块
# Import socket module and sys module for network communication
import socket  
import sys  

#create class TupleSpaceClient 
# 创建TupleSpaceClient类
class TupleSpaceClient:
    def __init__(self, host, port, request_file):
        # Initialize the client
        # 初始化客户端
        # 服务器主机地址,设置主机、端口和请求文件
        # Server host address
        self.host = host  
        self.port = port 
        # 请求文件名
        # Request file name
        self.request_file = request_file 
    
    # Main method to run the client
    # 运行客户端的主要方法
    def run(self):
        try:
            # Open and read the request file
            # 打开并读取请求文件
            with open(self.request_file, 'r') as file:
                requests = file.readlines()

            # Create a TCP socket and connect to the server
            # 创建TCP套接字并连接到服务器
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.host, self.port))

                # Process each request line in the file
                # 处理文件中的每个请求行
                for line in requests:
                    line = line.strip()
                    if not line:
                        continue
