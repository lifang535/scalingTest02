all_workers = 60
split = 500
over_provisioning = 1.1

# 20400:20499
client_address = "localhost:20400"
frontend_address = "localhost:20401"
worker_addresses = [f"localhost:2040{i}" for i in range(2, all_workers + 2)]
