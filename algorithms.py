from random import *

def leader_election_simple(brokers):
    minimal_hash = float('inf')
    for broker in brokers:
        h = hash(broker)
        if h < minimal_hash:
            minimal_hash = h
            leader = broker
    return leader

def SALSA_selection(requests, services):
    # TODO: implement
    return None

def greedy_selection(requests, services):
    # TODO: implement
    return None
