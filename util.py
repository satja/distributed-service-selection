from random import *
import geopy.distance

KM_PER_MS = 100

def leader_election_simple(brokers):
    minimal_hash = float('inf')
    for broker in brokers:
        h = hash(broker)
        if h < minimal_hash:
            minimal_hash = h
            leader = broker
    return leader

def distance_time(location1, location2):
    return geopy.distance.distance(location1, location2).km / KM_PER_MS

def random_location():
    return uniform(-90, 90), uniform(-180, 180)


if __name__ == '__main__':
    a = [random_location() for i in range(1000)]
    s = 0
    for i in range(1000):
        d = distance_time(choice(a), choice(a))
        s += d
        print(d, s / (i + 1))
