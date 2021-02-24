import algorithms
import logging
from util import *

from master_broker import MasterBroker

class Broker:
    def __init__(self, location, master_broker,
            selection_algorithm=algorithms.random_selection):
        self.location = location
        self.users = []
        self.services = dict()
        self.requests = []
        self.master_broker = master_broker
        self.selection_algorithm = selection_algorithm
        self.failed = False
        self.id = get_uid()
        self.master_broker.new_broker(self)
        self.balancing = self.master_broker.balancing
        self.alg_duration = 0
        logging.debug(f'0, broker, {self.id}, __init__, {location}, {master_broker.id}')

    def total_throughput(self):
        return sum(self.services.values())

    def get_users(self):
        return self.users[:]

    def get_services(self):
        return self.services.items()

    def update_master(self, master_broker):
        self.master_broker = master_broker
        logging.debug(f'0, broker, {self.id}, update_master, {master_broker.id}')

    def fail(self):
        self.failed = True
        logging.info(f'0, broker, {self.id}, fail')

    def is_failed(self):
        return self.failed

    def add_user(self, user):
        self.users.append(user)
        logging.debug(f'0, broker, {self.id}, add_user, {user.id}')

    def remove_user(self, user):
        self.users.remove(user)
        logging.debug(f'0, broker, {self.id}, remove_user, {user.id}')

    def update_brokers_list(self, brokers):
        self.brokers = brokers[:]
        logging.debug(f'0, broker, {self.id}, update_brokers_list')

    def update_service(self, service, throughput):
        assert throughput >= 0
        logging.info(f'0, broker, {self.id}, update_service, {service.id}, {throughput}')
        if throughput == 0:
            self.services.pop(service, None)
        else:
            self.services[service] = throughput

    def new_request(self, user, request_time):
        self.requests.append((user, request_time))
        logging.debug(f'{request_time}, broker, {self.id}, new_request, {user.id}')

    def perform_selection(self, begin_time):
        request_service, service_loads, selection_done_time = self.selection_algorithm(
                self.requests, self.services.items(), self, begin_time)

        reqs_qos = []
        unsatisfied_users = set()
        unsatisfied_rt = unsatisfied_rel = unsatisfied_both = 0
        unanswered_reqs = 0
        cost = 0
        for i, (user, request_time) in enumerate(self.requests):
            service = request_service[i]
            if not service:
                unsatisfied_users.add(user)
                unanswered_reqs += 1
                reqs_qos.append(None)
                continue
            service_time = selection_done_time +\
                    distance_time(self.location, user.location) +\
                    distance_time(user.location, service.location)
            qos = service.serve_request(user, request_time, service_time)
            if qos == None:
                self.master_broker.service_fail_report(service)
                logging.debug(f'{selection_done_time}, broker, {self.id}, detect_service_fail, {service.id}')
            else:
                reqs_qos.append(qos)
                cost += qos[2]
                if qos[0] < user.min_reliability and qos[1] > user.max_response_time:
                    unsatisfied_users.add(user)
                    unsatisfied_both += 1
                elif qos[0] < user.min_reliability:
                    unsatisfied_users.add(user)
                    unsatisfied_rel += 1
                elif qos[1] > user.max_response_time:
                    unsatisfied_users.add(user)
                    unsatisfied_rt += 1
        self.requests = []
        logging.debug(f'{begin_time}, broker, {self.id}, perform_selection, {selection_done_time}, {len(unsatisfied_users)}, {len(reqs_qos)}')

        # check for failed services
        for service in list(self.services.keys()):
            if service.is_failed():
                self.master_broker.service_fail_report(service)

        # report to master
        if not self.master_broker.selection_report(self, service_loads,\
                unsatisfied_users, unanswered_reqs):
            # master failed -> leader election among brokers
            new_master = leader_election_simple(self.brokers)
            if new_master == self:
                logging.debug(f'{selection_done_time}, broker, {self.id}, elected')
                master_broker = MasterBroker(self.location, self.balancing)
                self.failed = True
                master_broker.fill_brokers_data(self.brokers)

        unsatisfied_reqs = unsatisfied_rel, unsatisfied_rt, unsatisfied_both
        return reqs_qos, unanswered_reqs, unsatisfied_reqs, cost, service_loads
