from random import *
import logging
from time import time
from collections import defaultdict, deque

def SALSA_selection(requests, services):
    # TODO: implement
    return None

def greedy_selection(requests, services, broker, begin_time):
    start = time()
    request_service = []
    service_loads = defaultdict(int)
    service_throughput = list(services)
    logging.debug([(s.id, load) for s, load in services])
    for user, request_time in requests:
        if not service_throughput:
            request_service.append(None)
            continue
        best_utility_cost = float('inf')
        best_i = -1
        for i, (service, thr) in enumerate(service_throughput):
            utility_cost = service.utility_cost(user, request_time, begin_time)
            if utility_cost < best_utility_cost:
                best_utility_cost = utility_cost
                best_i = i
        service, thr = service_throughput[best_i]
        request_service.append(service)
        service_loads[service] += 1
        if thr > 1:
            service_throughput[best_i] = (service, thr - 1)
        else:
            service_throughput.pop(best_i)
    duration_ms = (time() - start) * 1000
    logging.debug([(s.id, load) for s, load in service_loads.items()])
    logging.info(f'0, greedy_selection, {len(requests)}, {len(services)}, {duration_ms}, {broker.id}, {broker.total_throughput()}')
    return request_service, service_loads, duration_ms

def round_robin_selection(requests, services, broker, begin_time):
    start = time()
    queue = deque(services)
    service_loads = defaultdict(int)
    request_service = []
    for r in requests:
        if not queue:
            break
        service, thr = queue.popleft()
        request_service.append(service)
        service_loads[service] += 1
        if thr > 1:
            queue.append((service, thr - 1))
    request_service.extend([None] * max(0, len(requests) - len(request_service)))
    duration_ms = (time() - start) * 1000
    logging.info(f'0, round_robin_selection, {len(requests)}, {len(services)}, {duration_ms}, {broker.id}, {broker.total_throughput()}')
    return request_service, service_loads, duration_ms

def random_selection(requests, services, broker, begin_time):
    start = time()
    service_units = []
    for service, throughput in services:
        service_units.extend([service] * throughput)
    service_units.extend([None] * max(0, len(requests) - len(service_units)))
    request_service = sample(service_units, len(requests))
    service_loads = defaultdict(int)
    for service in request_service:
        service_loads[service] += 1
    service_loads.pop(None, None)
    duration_ms = (time() - start) * 1000
    logging.info(f'0, random_selection, {len(requests)}, {len(services)}, {len(service_units)}, {duration_ms}, {broker.id}, {broker.total_throughput()}')
    return request_service, service_loads, duration_ms

def ap_selection(requests, services):
    # TODO: implement
    return None

def tp_selection(requests, services):
    # TODO: implement
    return None
