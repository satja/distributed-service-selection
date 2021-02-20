import logging
from util import *

class Service:
    def __init__(self, location, throughput, reliability, computation_time, cost, premium_cost):
        self.location = location
        self.reliability = reliability
        self.throughput = throughput
        self.computation_time = computation_time
        self.cost = cost
        self.premium_cost = premium_cost
        self.failed = False
        self.id = get_uid()
        logging.debug(f'0, service, {self.id}, __init__, {location}, {throughput}, {reliability}, {computation_time}, {cost}')

    def fail(self):
        self.failed = True
        logging.info(f'0, service, {self.id}, fail')
    
    def is_failed(self):
        return self.failed

    def cost_to_user(self, user):
        if user.premium:
            return self.premium_cost 
        return self.cost

    def utility_cost(self, user, request_time, arrival_time):
        if self.failed:
            return INFEASIBLE
        answer_time = arrival_time + self.computation_time +\
                distance_time(self.location, user.location)
        response_time = answer_time - request_time
        if response_time <= user.max_response_time and self.reliability >= user.min_reliability:
            return self.cost_to_user(user)
        return MAX_COST + self.cost_to_user(user)

    def serve_request(self, user, request_time, arrival_time):
        if self.failed:
            return None
        answer_time = arrival_time + self.computation_time +\
                distance_time(self.location, user.location)
        response_time = answer_time - request_time
        logging.debug(f'{arrival_time}, service, {self.id}, serve_request, {user.id}, {request_time}, {answer_time}, {response_time}')
        return (self.reliability, response_time, self.cost_to_user(user))
