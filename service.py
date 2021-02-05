from util import distance_time

class Service:
    def __init__(self, location, throughput, reliability, computation_time, cost):
        self.location = location
        self.reliability = reliability
        self.throughput = throughput
        self.computation_time = computation_time
        self.cost = cost

    def get_response_time(self, user):
        return self.computation_time + distance_time(self.location, user.location)

    def serve_request(user, request_time, begin_time):
        final_time = begin_time + self.computation_time +\
                distance_time(self.location, user.location)
        response_time = final_time - request_time
        return (self.reliability, self.response_time, self.cost)
