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
                    
                    # Parse the request line into parts
                    # 将请求行分割成多个部分
                    parts = line.split()
                    if len(parts) < 2:
                        print(f"Invalid request line: {line}")
                        continue

                    # Extract operation, key, and value from the request line
                    # 从请求行中提取操作、键和值
                    #operation
                    operation = parts[0].upper()
                    #key
                    key = ' '.join(parts[1:-1]) if operation == 'PUT' else ' '.join(parts[1:])
                    #value
                    value = parts[-1] if operation == 'PUT' else None

                    # Validate the collated size for PUT operations
                    # 验证PUT操作的合并大小
                    if operation == 'PUT':
                        collated_size = len(key) + len(value) + 1  # +1 for space
                        if collated_size > 970:
                            print(f"Error: collated size exceeds limit for line: {line}")
                            continue
                    # Prepare the request message based on the operation
                    # 根据操作类型准备请求消息
                    #Check if the operation is 'READ'
                    if operation == 'READ':
                        # Set command to 'R' for READ operation
                        cmd = 'R'
                        # Format the message as "R <key>"
                        message = f"{cmd} {key}"
                    
                    # If operation is not 'READ', check if it's 'GET'
                    # 如果操作不是'READ'，检查是否是'GET'
                    elif operation == 'GET':
                        # Set command to 'G' for GET operation
                        # 为GET操作设置命令为'G'
                        cmd = 'G'
                        # Format the message as "G <key>"
                        # 将消息格式化为"G <key>"
                        message = f"{cmd} {key}"
                    
                    # If operation is not 'READ' or 'GET', check if it's 'PUT'
                    # 如果操作不是'READ'或'GET'，检查是否是'PUT'
                    elif operation == 'PUT':
                        # Set command to 'P' for PUT operation
                        # 为PUT操作设置命令为'P'
                        cmd = 'P'
                        # Format the message as "P <key> <value>"
                        # 将消息格式化为"P <key> <value>"
                        message = f"{cmd} {key} {value}"

                    # If operation is none of the above
                    # 如果操作不是以上任何一种
                    else:
                        # Print error message for invalid operation
                        # 打印无效操作的错误信息
                        print(f"Invalid operation: {operation}")
                        # Skip to next iteration of the loop
                        # 跳过本次循环，继续下一次
                        continue

                    # Calculate the message length and format the request
                    # 计算消息长度并格式化请求
                    size = len(message) + 4  # 3 for size digits + 1 space
                    request_msg = f"{size:03d} {message}"

                    # Send the request to the server
                    # 向服务器发送请求
                    sock.sendall(request_msg.encode('utf-8'))

                    # Receive the response from the server
                    # 接收服务器的响应
                    response = sock.recv(1024).decode('utf-8').strip()

                    # Parse the response into size and message
                    # 将响应解析为大小和消息
                    response_parts = response.split(' ', 1)
                    # Validate that the response contains both size and message parts
                    # If not, print error and skip to next request
                    # 验证响应是否包含大小和消息两部分
                    #如果不包含，打印错误并跳过处理下一个请求
                    if len(response_parts) < 2:
                        print(f"Invalid response format: {response}")
                        continue
                    # Extract the response size (should be 3-digit number)
                    # 提取响应大小（应该是3位数字）
                    response_size = response_parts[0]
                    # Extract the actual response message content
                    # This is everything after the first space
                    # 提取实际的响应消息内容
                    # 这是第一个空格后的所有内容
                    response_msg = response_parts[1]
        # Handle file not found error
        # 处理文件未找到错误
        except FileNotFoundError:
            print(f"Error: File not found - {self.request_file}")

        # Handle connection refused error
        # 处理连接被拒绝错误
        except ConnectionRefusedError:
            print("Error: Could not connect to the server")

# Main function to start the client
# 主函数，启动客户端
def main():
    # Check if the correct number of arguments is provided
    # 检查是否提供了正确数量的参数
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <request_file>")
        return
    
    # Get host from command line arguments
    # 从命令行参数获取主机
    host = sys.argv[1]


                    
