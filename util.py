
def leader_election_simple(brokers):
    minimal_hash = float('inf')
    for broker in brokers:
        h = hash(broker)
        if h < minimal_hash:
            minimal_hash = h
            leader = broker
    return leader

def distance_time(location1, location2):
    return 0

def random_location():
    return (0, 0)
