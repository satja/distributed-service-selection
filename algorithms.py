from random import *
import logging
from time import time
from collections import defaultdict, deque

def SALSA_selection(requests, services):
    # TODO: implement
    return None

def greedy_selection(requests, services):
    # TODO: implement
    return None

def round_robin_selection(requests, services, broker_id):
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
    logging.info(f'0, random_selection, {len(requests)}, {len(services)}, {duration_ms}, {broker_id}')
    return request_service, service_loads, duration_ms

def random_selection(requests, services, broker_id):
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
    logging.info(f'0, random_selection, {len(requests)}, {len(services)}, {len(service_units)}, {duration_ms}, {broker_id}')
    return request_service, service_loads, duration_ms

def ap_selection(requests, services):
    # TODO: implement
    return None

def tp_selection(requests, services):
    # TODO: implement
    return None
