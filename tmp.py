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
        logging.debug(f'0, broker, {self.id}, __init__, {location}, {master_broker.id}')

    def total_throughput(self):
        return sum(thr for service, thr in self.services.items())

    def get_users(self):
        return self.users[:]

    def get_services(self):
        return self.services

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

    def add_service(self, service, throughput):
        #self.services.append((service, throughput))
        self.services[service] = throughput
        logging.info(f'0, broker, {self.id}, add_service, {service.id}, {throughput}')

    def update_brokers_list(self, brokers):
        self.brokers = brokers[:]
        logging.debug(f'0, broker, {self.id}, update_brokers_list')

    def update_service(self, service, throughput):
        assert throughput >= 0
        logging.info(f'0, broker, {self.id}, update_service, {service.id}, {throughput}')
        if throughput == 0:
            self.services.pop(service)
        else:
            self.services[service] = throughput
        return
        for i, (s, r) in enumerate(self.services):
            if s == service:
                if throughput == 0:
                    self.services.pop(i)
                else:
                    self.services[i] = (service, throughput)
                return
        self.services.append((service, throughput))

    def new_request(self, user, request_time):
        self.requests.append((user, request_time))
        logging.debug(f'{request_time}, broker, {self.id}, new_request, {user.id}')

    def perform_selection(self, begin_time):
        request_service, service_loads, duration =\
                self.selection_algorithm(self.requests, self.services, self)

        selection_done_time = begin_time + duration
        reqs_qos = []
        unsatisfied_users = set()
        unsatisfied_reqs = 0
        for i, (user, request_time) in enumerate(self.requests):
            service = request_service[i]
            if not service:
                unsatisfied_users.add(user)
                unsatisfied_reqs += 1
                reqs_qos.append(None)
                continue
            service_time = selection_done_time + distance_time(
                    self.location, service.location)
            qos = service.serve_request(user, request_time, service_time)
            if qos == None:
                self.master_broker.service_fail_report(service)
                logging.debug(f'{selection_done_time}, broker, {self.id}, detect_service_fail, {service.id}')
            else:
                reqs_qos.append(qos)
                if qos[0] < user.min_reliability or qos[1] > user.max_response_time:
                    unsatisfied_users.add(user)
                    unsatisfied_reqs += 1
        self.requests = []
        logging.debug(f'{begin_time}, broker, {self.id}, perform_selection, {selection_done_time}, {len(unsatisfied_users)}, {len(reqs_qos)}')

        # check for failed services
        for service in list(self.services.keys()):
            if service.is_failed():
                self.master_broker.service_fail_report(service)

        # report to master
        if not self.master_broker.selection_report(self, service_loads, unsatisfied_users):
            # master failed -> leader election among brokers
            new_master = leader_election_simple(self.brokers)
            if new_master == self:
                logging.debug(f'{selection_done_time}, broker, {self.id}, elected')
                master_broker = MasterBroker(self.location)
                self.failed = True
                master_broker.fill_brokers_data(self.brokers)

        return reqs_qos, unsatisfied_reqs, service_loads
