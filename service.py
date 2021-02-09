from util import distance_time

class Service:
    def __init__(self, location, throughput, reliability, computation_time, cost):
        self.location = location
        self.reliability = reliability
        self.throughput = throughput
        self.computation_time = computation_time
        self.cost = cost
        self.failed = False

    def fail(self):
        self.failed = True

    def serve_request(user, request_time, arrival_time):
        if self.failed:
            return None
        answer_time = arrival_time + self.computation_time +\
                distance_time(self.location, user.location)
        response_time = answer_time - request_time
        return (self.reliability, self.response_time, self.cost)
