import grpc
import time
from math import ceil

import threading
import multiprocessing
from concurrent import futures

import _utils
import simulation_pb2, simulation_pb2_grpc
from _config import client_address, frontend_address, worker_addresses, all_workers, over_provisioning
from worker import Worker

class Frontend(simulation_pb2_grpc.InferenceServicer):
    # 初始化 Frontend
    def __init__(self, workers, server, duration=0.4, batch_size=4, num_workers=0, status=True):
        # 用 ProcessBatch 向 Workers 发送请求
        self.worker_addresses = worker_addresses
        self.channels = [grpc.insecure_channel(address) for address in self.worker_addresses] 
        self.stubs = [simulation_pb2_grpc.InferenceStub(channel) for channel in self.channels]

        # 用 SendRequest 向 Client 发送请求
        self.client_address = client_address
        self.channel = grpc.insecure_channel(client_address)
        self.stub = simulation_pb2_grpc.InferenceStub(self.channel)

        self.workers = workers  # 存储 worker 的信息
        self.server = server  # 用于停止 Frontend
        self.duration = duration  # 每个 batch 的处理时间
        self.batch_size = batch_size  # 每个 batch 中包含的 requests 的数量
        self.num_workers = num_workers  # 工作的 worker 数量
        self.status = status  # 控制两个线程同时停止

        self.queue = []  # 所有的 requests
        self.times = []  # 用于测量 rate
        self.threadings = []   # 用于存储线程
        
        self.rate = 0  # 用于展示所测量 rate

    def SendRequest(self, request, context):
        if request.message_type == 1:
            self.times.append(time.time())

            # 第一次调用 SendRequest 时打开 monitor 线程，show_parameters 线程以及 monitor_queue 线程
            if self.num_workers == 0:
                frontend_monitor = threading.Thread(target=self.monitor)
                frontend_monitor.start()
                self.num_workers += 1

                frontend_show_parameters = threading.Thread(target=self.show_parameters)
                frontend_show_parameters.start()
                
                frontend_monitor_queue = threading.Thread(target=self.monitor_queue)
                frontend_monitor_queue.start()

            self.queue.append(request)
            return simulation_pb2.Response()
        
        elif request.message_type == 3:
            self.workers[request.id - 1].is_working = request.worker_status    
            return simulation_pb2.Response()
        elif request.message_type == 6:
            with threading.Lock():
                print("[Frontend] server is stopped")
            self.status = False
            return simulation_pb2.Response()

    def monitor_queue(self):
        with threading.Lock():
            print("[Frontend] monitor_queue is working")
        while True:
            if not self.status:
                break
            if len(self.queue) >= self.batch_size:
                batch = self.queue[:self.batch_size]
                self.queue = self.queue[self.batch_size:]

                send_batches = threading.Thread(target=self.send_batches, args=(batch,))
                send_batches.start()

                #self.send_batches(batch)

            time.sleep(0.01)
    

    def send_batches(self, batch):
        i = self.select_worker()
        requests = simulation_pb2.Batch(requests=batch)
        batch_response = self.stubs[i].ProcessBatch(requests)

        #batch_request = simulation_pb2.Request(message_type=2, requests=batch)
        #batch_response = self.stubs[i].SendRequest(batch_request)
        end_times = batch_response.end_times
        adj_worker_times = batch_response.adj_worker_times
        for end_time, adj_worker_time in zip(end_times, adj_worker_times):
            request = simulation_pb2.Request(message_type=4, data_time=end_time, adj_worker_time=adj_worker_time)
            response = self.stub.SendRequest(request)
            #self.stub.SendEndTime(simulation_pb2.EndTime(time=end_time, adj_worker_time=adj_worker_time))

        #return batch_response.end_times

    # Frontend 的监测器 rate and stop_flag
    def monitor(self):
        with threading.Lock():
            print("[Frontend] monitor is working")
        rates = []
        timer_show_num_workers = time.time()
        while True:
            if not self.status:
                self.num_workers = 0
                time.sleep(0.1)
                
                for stub in self.stubs:
                    # 发送结束信号 -1
                    request = simulation_pb2.Request(message_type=7)
                    response = stub.SendRequest(request)
                    #stub.SendRequest(simulation_pb2.Request(start_time=-1))

                self.server.stop(0)
                with threading.Lock(): 
                    print("[Frontend] monitor is stopped")
                break
            if len(self.times) >= self.batch_size:
                rate = self.batch_size / (self.times[self.batch_size - 1] - self.times[0])
                rates.append(rate)
                self.times = self.times[self.batch_size:]
            if len(rates) >= 1:
                self.rate = ceil(sum(rates) / len(rates))
                self.num_workers = max(ceil(self.duration * (sum(rates) / len(rates)) / self.batch_size), 1)  # 调整工作的 worker 数量
                rates = []
            now = time.time()
            if now - timer_show_num_workers >= 0.1:
                timer_show_num_workers = time.time()
                request = simulation_pb2.Request(message_type=5, num_workers=ceil(self.num_workers * over_provisioning))
                response = self.stub.SendRequest(request)
                #self.stub.SendRequest(simulation_pb2.Request(id=-2, num_workers=self.num_workers + 1))
                #print("[Frontend] sent the number of workers:", self.num_workers, ", to the client")
            time.sleep(0.01)

    def show_parameters(self):
        while True:
            if not self.status:
                break
            with threading.Lock():
                print(f"[Frontend] the measured rate = {self.rate}")
                print(f"[Frontend] the number of workers = {min(ceil(self.num_workers * over_provisioning), len(self.workers))}")
            time.sleep(1)

    # 选择 worker
    def select_worker(self):  # 问题：num_workers 向上调整过大
        for i in range(min(ceil(self.num_workers * over_provisioning), len(self.workers))):
        # for i in range(len(self.workers)):
            if not self.workers[i].is_working:
                # return self.workers[i]
                return i

frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=all_workers + 2))

def start_frontend_server(frontend_address, is_end):
    #frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    workers = [Worker(f"Worker-{i+1}", grpc.server(futures.ThreadPoolExecutor(max_workers=10)), worker_addresses[i]) for i in range(all_workers)]
    frontend = Frontend(workers, frontend_server)
    for worker in workers:
        simulation_pb2_grpc.add_InferenceServicer_to_server(frontend, frontend_server)
    frontend_server.add_insecure_port(frontend_address)
    frontend_server.start()
    """
    while True:
        time.sleep(0.01)
        if is_end.value:
            frontend_server.stop(0)
            break
    """
    frontend_server.wait_for_termination()
    

if __name__ == "__main__":
    is_end = multiprocessing.Value("i", 0)
    multiprocessing.Process(target=start_frontend_server, args=(frontend_address, is_end,)).start()
