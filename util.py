from random import *
import math

KM_PER_MS = 100
MAX_COST = 100
INFEASIBLE = 10**8
TRANSFER_UNIT = 1

uid = 0
def get_uid():
    global uid
    uid += 1
    return uid

def leader_election_simple(brokers):
    minimal_hash = float('inf')
    for broker in brokers:
        h = hash(broker)
        if h < minimal_hash:
            minimal_hash = h
            leader = broker
    return leader

def distance_time(location1, location2):
    lat1, lon1 = location1
    lat2, lon2 = location2
    radius = 6371  # km
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d / KM_PER_MS

def random_location():
    return uniform(-90, 90), uniform(-180, 180)

def sigmoid(x):
  return 1 / (1 + math.exp(-x))


if __name__ == '__main__':
    a = [random_location() for i in range(1000)]
    s = 0
    for i in range(1000):
        d = distance_time(choice(a), choice(a))
        s += d
        print(d, s / (i + 1))
