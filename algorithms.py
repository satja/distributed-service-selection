from random import *
import logging

def SALSA_selection(requests, services):
    # TODO: implement
    return None

def greedy_selection(requests, services):
    # TODO: implement
    return None

def random_selection(requests, services):
    duration = 0
    service_units = []
    for service, throughput in services:
        service_units.extend([service] * throughput)
    request_service = sample(service_units, len(requests))
    service_loads = [request_service.count(s) for s in services]
    return request_service, service_loads, duration
