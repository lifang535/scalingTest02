import grpc
import time
import random
from math import ceil
from math import sin, cos
from datetime import datetime  
import matplotlib.pyplot as plt
import numpy as np
import asyncio

import threading
import multiprocessing
from concurrent import futures

import _utils
import simulation_pb2, simulation_pb2_grpc
from _config import client_address, frontend_address, worker_addresses, split

class Client(simulation_pb2_grpc.InferenceServicer):
    # 初始化 Client
    def __init__(self, frontend_address, frontend_list, backend_list, rate, server, show_num_workers, adj_worker_times, total=1e4, batch_size=4, is_running=True):
        # 用 SendRequest 向 Frontend 发送请求
        self.frontend_address = frontend_address
        self.channel = grpc.insecure_channel(frontend_address)
        self.stub = simulation_pb2_grpc.InferenceStub(self.channel)

        self.frontend_list = frontend_list  # 存储请求处理开始的时间
        self.backend_list = backend_list  # 存储请求处理结束的时间
        #self.frontend_list = []
        #self.backend_list = []
        self.rate = rate  # 控制发送请求的速率
        self.server = server  # 用于停止 Client
        self.total = int(total)  # 发送请求的总数
        self.batch_size = batch_size  # 每个 batch 中包含的 requests 的数量
        self.is_running = is_running  # 控制两个线程同时停止
        self.end_flag = False  # 标记是否收到服务结束信号

        self.latencies = []  # 存储每个 request 的 latency
        self.start_adj = time.time()
        self.end_adj = time.time()
        self.adj_times = []
        self.adj_worker_times = adj_worker_times

        self.show_latencies = []
        self.show_rates = []
        self.show_p99_latencies = []
        self.show_num_workers = show_num_workers

    def run(self):
        # 通过 self.is_running 控制线程同时停止
        client_send_requests = threading.Thread(target=self.send_requests)
        #client_dynamic_rate = threading.Thread(target=self.dynamic_rate)
        client_dynamic_rate = threading.Thread(target=self.smooth_rate)
        client_monitor = threading.Thread(target=self.monitor)
        client_send_requests.start()
        client_dynamic_rate.start()
        client_monitor.start()
        while True:
            time.sleep(0.01)
            if self.end_flag:
                self.draw_data()
                return

    # 向 Frontend 发送请求
    def send_requests(self):
        # 获取当前日期时间
        now = datetime.now()
        datetime_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # 打开文件
        file = open("../log.txt", "a")

        # 写入内容
        file.write("Current datetime is: " + datetime_str + "\n")
        file.write("This is a new test.\n\n")

        # 关闭文件
        file.close()

        for i in range(self.total):
            if not self.is_running and i % self.batch_size == 0:
                break

            #time.sleep(max(1 / self.rate - (self.end_adj - self.start_adj), 0))  # 动态调整改为后处理
            time.sleep(1 / self.rate)
            self.start_adj = time.time()

            start_time = int(time.time() * 1e6) 
            self.frontend_list.append(start_time)  # 记录请求处理开始的时间
            # 同步 API
            request = simulation_pb2.Request(message_type=1, id=i, data_time=start_time)  # 构造 Request
            response = self.stub.SendRequest(request)

            self.end_adj = time.time()

            self.adj_times.append(self.end_adj - self.start_adj)

        with threading.Lock():
            print("[Client] all requests were sent")

        self.is_running = False

        while True:  # 等待所有返回结果
            time.sleep(0.01)
            #print(len(self.backend_list), " ", len(self.frontend_list))
            #print(self.backend_list[0], " ", self.frontend_list[0])
            if len(self.backend_list) == len(self.frontend_list):
                break
        
        # 发送结束信号 -1
        request = simulation_pb2.Request(message_type=6)
        # self.frontend_list.append(request.start_time)
        response = self.stub.SendRequest(request)

        # 记录 latencies
        if min(len(self.backend_list), len(self.frontend_list)) > 0:
            for i in range(min(len(self.backend_list), len(self.frontend_list))):
                self.latencies.append((self.backend_list[i] - self.frontend_list[i]) / 1e3)
            self.print_statistics()
        
        with threading.Lock():
            print("[Client] server is stopped")

        self.end_flag = True

        file = open("../log.txt", "a")
        file.write("\n")
        file.close()

    def monitor(self):
        while True:
            time.sleep(0.01)
            if not self.is_running:
                break
            if len(self.backend_list) >= split and len(self.frontend_list) >= split and len(self.adj_times) >= split and len(self.adj_worker_times) >= split:  #  and len(self.adj_worker_times) >= split
                for _ in range(int(split)):
                    #print("123123", self.backend_list[0], " ", self.frontend_list[0], " ", self.adj_times[0], " ", self.adj_worker_times[0])
                    self.latencies.append((self.backend_list[0] - self.frontend_list[0] - self.adj_times[0] * 1e6 - self.adj_worker_times[0]) / 1e3)  #  - self.adj_worker_times[0]
                    self.backend_list.pop(0)
                    self.frontend_list.pop(0)
                    self.adj_times.pop(0)
                    self.adj_worker_times.pop(0)
                self.print_statistics()
                self.latencies = self.latencies[int(split):]

    def SendRequest(self, request, context):
        if request.message_type == 4:
            self.backend_list.append(request.data_time)
            self.adj_worker_times.append(request.adj_worker_time)
            return simulation_pb2.Response()
        elif request.message_type == 5:
            self.show_num_workers.append(request.num_workers)
        return simulation_pb2.Response()

    # 动态调整 rate
    def dynamic_rate(self):
        for i in range(2):
            for j in range(9):  # 9
                time.sleep(1)  # 10 / 1
                if not self.is_running:
                    return
                self.rate -= 10 * (i * 2 - 1)  # 10 -> 100 -> 10
                with multiprocessing.Lock():
                    print("[Client] self.rate.value =", self.rate)

        self.is_running = False
    
    # 生成随机数变化 rate
    def smooth_rate(self):
        k1 = random.randint(50, 60)
        k2 = random.randint(50, 60)
        k3 = random.randint(120, 150)
        k4 = random.randint(1, 10)
        timer_print = time.time()
        timer_dynamic = time.time()
        timer_show_rate = time.time()
        while True:
            time.sleep(0.01)
            if not self.is_running:
                break
            now = time.time()
            if now - timer_print >= 1.0:
                timer_print = now
                with multiprocessing.Lock():
                    print("[Client] self.rate.value =", self.rate)
            if now - timer_dynamic >= 10:
                timer_dynamic = now
                k1 = random.randint(50, 60)
                k2 = random.randint(50, 60)
                k3 = random.randint(100, 120)
            self.rate = int(k1 * sin(time.time()) + k2 * cos(time.time()) + k3 + k4)
            if now - timer_show_rate >= 0.1:
                timer_show_rate = now
                self.show_rates.append(self.rate)
            k4 = random.randint(1, 10)

    # 打印数据
    def print_statistics(self):
        self.show_latencies.extend(self.latencies)

        with threading.Lock():
            print("[Client] request latencies =")
            for i in range(len(self.latencies)):
                print(round(self.latencies[i]), end=" ")
                if (i + 1) % 20 == 0:
                    print()
            if len(self.latencies) % 20 != 0:
                print()
            print("[Client] amount of requests =", len(self.latencies))
        self.latencies.sort()
        avg_latency = sum(self.latencies) / len(self.latencies)
        p99_latency = self.latencies[ceil(0.99 * len(self.latencies)) - 1]
        with threading.Lock():
            print(f"max of latencies: {self.latencies[len(self.latencies) - 1]:.1f} ms")
            print(f"avg of latencies: {avg_latency:.3f} ms")
            print(f"99% quantile of latencies: {p99_latency:.1f} ms")
        
        self.show_p99_latencies.append(p99_latency)

        # 获取当前日期时间
        now = datetime.now()
        datetime_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # 打开文件，如果文件不存在则创建
        file = open("../log.txt", "a")

        # 写入内容
        file.write("Current datetime is: " + datetime_str + "\n")
        file.write("amount of requests: " + str(len(self.latencies)) + "\n")
        file.write("max of latencies: {:.1f} ms\n".format(self.latencies[len(self.latencies) - 1]))
        file.write("avg of latencies: {:.3f} ms\n".format(avg_latency))
        file.write("99% quantile of latencies: {:.1f} ms\n\n".format(p99_latency))

        # 关闭文件
        file.close()

    # 绘制图形
    def draw_data(self):
        # 创建图形窗口
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(nrows=4, ncols=1, sharex=False, figsize=(8, 8))

        # 调整子图之间的距离
        plt.subplots_adjust(hspace=0.5, wspace=0.5)

        # 绘制折线图1
        x1 = np.arange(len(self.show_latencies))
        y1 = self.show_latencies
        ax1.plot(x1, y1)
        ax1.set_ylabel('Latencies')
        ax1.set_xlabel('RID')
        ax1.autoscale()

        # 绘制折线图2
        x2 = np.arange(len(self.show_rates))
        y2 = self.show_rates
        ax2.plot(x2, y2)
        ax2.set_ylabel('Rate')
        ax2.set_xlabel('Time')
        ax2.autoscale()

        # 绘制折线图3
        x3 = np.arange(len(self.show_p99_latencies))
        y3 = self.show_p99_latencies
        ax3.plot(x3, y3)
        ax3.set_ylabel('P99 Latencies')
        ax3.set_xlabel('Time')
        ax3.autoscale()

        # 绘制折线图4
        x4 = np.arange(len(self.show_num_workers))
        #print(len(self.show_num_workers))
        y4 = self.show_num_workers
        #print(self.show_num_workers)
        ax4.plot(x4, y4)
        ax4.set_ylabel('Num Workers')
        ax4.set_xlabel('Time')
        ax4.autoscale()

        # 显示图形
        plt.show()

client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

def start_client_server(client_address, is_end):
    #client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    simulation_pb2_grpc.add_InferenceServicer_to_server(Client(client_address, frontend_list, backend_list, rate, client_server, show_num_workers, adj_worker_times), client_server)
    client_server.add_insecure_port(client_address)
    client_server.start()
    while True:
        time.sleep(0.01)
        if is_end.value:
            client_server.stop(0)
            break
    #client_server.wait_for_termination()

if __name__ == "__main__":
    manager_1 = multiprocessing.Manager()
    manager_2 = multiprocessing.Manager()
    manager_3 = multiprocessing.Manager()
    manager_4 = multiprocessing.Manager()
    frontend_list = manager_1.list()  # 存储请求处理开始的时间
    backend_list = manager_2.list()  # 存储请求处理结束的时间
    show_num_workers = manager_3.list()  # 存储 num_workers 的值
    adj_worker_times = manager_4.list()  # 存储调整 worker 处理的时间
    #rate = multiprocessing.Value('i', 10)
    rate = 10
    is_end = multiprocessing.Value('b', False)

    multiprocessing.Process(target=start_client_server, args=(client_address, is_end,)).start()

    time.sleep(1)
    client = Client(frontend_address, frontend_list, backend_list, rate, grpc.server(futures.ThreadPoolExecutor(max_workers=10)), show_num_workers, adj_worker_times)
    client.run()

    is_end.value = True

    time.sleep(0.1)
    print("All servers are stopped")
