import grpc
import time
from math import ceil

import threading
import multiprocessing
from concurrent import futures

import _utils
import simulation_pb2, simulation_pb2_grpc
from client import Client
from frontend import Frontend
from worker import Worker
from _config import client_address, frontend_address, worker_addresses, all_workers

client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=all_workers + 2))
worker_servers = []

def start_client_server(client_address, is_end):
    #client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    simulation_pb2_grpc.add_InferenceServicer_to_server(Client(client_address, frontend_list, backend_list, rate, client_server), client_server)
    client_server.add_insecure_port(client_address)
    client_server.start()
    while True:
        time.sleep(0.01)
        if is_end.value:
            client_server.stop(0)
            break

def start_frontend_server(frontend_address, is_end):
    #frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    workers = [Worker(f"Worker-{i+1}", grpc.server(futures.ThreadPoolExecutor(max_workers=10)), worker_addresses[i]) for i in range(all_workers)]
    frontend = Frontend(workers, frontend_server)
    for worker in workers:
        simulation_pb2_grpc.add_InferenceServicer_to_server(frontend, frontend_server)
    frontend_server.add_insecure_port(frontend_address)
    frontend_server.start()
    while True:
        time.sleep(0.01)
        if is_end.value:
            frontend_server.stop(0)
            break

def start_worker_servers(worker_addresses, is_end):
    #worker_servers = []
    for i, worker_address in enumerate(worker_addresses):
        worker_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        #worker = Worker(f"Worker-{i+1}", worker_server)
        #simulation_pb2_grpc.add_InferenceServicer_to_server(worker, worker_server)
        simulation_pb2_grpc.add_InferenceServicer_to_server(Worker(f"Worker-{i+1}", worker_server, worker_addresses[i]), worker_server)

        worker_server.add_insecure_port(worker_address)
        worker_server.start()

        print(f"Worker-{i+1} server started on {worker_address}")
        worker_servers.append(worker_server)
    while True:
        time.sleep(0.01)
        if is_end.value:
            for worker_server in worker_servers:
                worker_server.stop(0)
            break

        
if __name__ == "__main__":
    manager_1 = multiprocessing.Manager()
    manager_2 = multiprocessing.Manager()
    frontend_list = manager_1.list()  # 存储请求处理开始的时间
    backend_list = manager_2.list()  # 存储请求处理结束的时间
    #rate = multiprocessing.Value('i', 10)
    rate = 10
    is_end = multiprocessing.Value('b', False)

    multiprocessing.Process(target=start_client_server, args=(client_address, is_end,)).start()
    multiprocessing.Process(target=start_frontend_server, args=(frontend_address, is_end,)).start()
    multiprocessing.Process(target=start_worker_servers, args=(worker_addresses, is_end,)).start()

    time.sleep(1)
    client = Client(frontend_address, frontend_list, backend_list, rate, grpc.server(futures.ThreadPoolExecutor(max_workers=10)))
    client.run()
    is_end.value = True

    time.sleep(0.1)
    print("All servers are stopped")
