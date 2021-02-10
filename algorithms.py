from random import *
import logging
from time import time

def SALSA_selection(requests, services):
    # TODO: implement
    return None

def greedy_selection(requests, services):
    # TODO: implement
    return None

def random_selection(requests, services, broker_id):
    start = time()
    service_units = []
    for service, throughput in services:
        service_units.extend([service] * throughput)
    while len(service_units) < len(requests):
        service_units.append(None)
    request_service = sample(service_units, len(requests))
    service_loads = [request_service.count(s) for s in services]
    duration_ms = (time() - start) * 1000
    logging.info(f'0, random_selection, {len(requests)}, {len(services)}, {len(service_units)}, {duration_ms}, {broker_id}')
    return request_service, service_loads, duration_ms

def ap_selection(requests, services):
    # TODO: implement
    return None

def tp_selection(requests, services):
    # TODO: implement
    return None
