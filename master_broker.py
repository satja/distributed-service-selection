import random
from collections import defaultdict
import logging
from util import get_uid, distance_time

class MasterBroker:
    def __init__(self, location):
        self.location = location
        self.brokers = []
        self.user_to_broker = dict()
        self.broker_to_users = defaultdict(list)
        self.service_broker_throughput = defaultdict(int)
        self.broker_to_services = defaultdict(list)
        self.service_broker_load = defaultdict(int)
        self.total_broker_load = defaultdict(int)
        self.failed = False
        self.id = get_uid()
        logging.debug(f'0, master, {self.id}, __init__, {location}')

    def new_broker(self, broker):
        self.brokers.append(broker)
        logging.debug(f'0, master, {self.id}, new_broker, {broker.id}')
        return self.brokers[:]

    def fill_brokers_data(self, brokers):
        logging.debug(f'0, master, {self.id}, fill_brokers_data')
        # this happens when a new master is elected among brokers
        for broker in brokers:
            broker.update_master(self)
            self.brokers.append(broker)
            for user in broker.get_users():
                self.user_to_broker[user] = broker
                self.broker_to_users[broker].append(user)
            for service, throughput in broker.get_services():
                self.service_broker_throughput[(service, broker)] = service.throughput
                self.broker_to_services[broker].append(service)
        self.check_for_failed_brokers()

    def check_for_failed_brokers(self):
        logging.debug(f'0, master, {self.id}, check_for_failed_brokers')
        # detect failed brokers
        failed_brokers = [broker for broker in self.brokers if broker.is_failed()]
        self.brokers = [broker for broker in self.brokers if not broker.is_failed()]
        for broker in self.brokers:
            broker.update_brokers_list(self.brokers)

        # redistribute the users/services from failed brokers
        for broker in failed_brokers:
            self.total_broker_load.pop(broker)
            for user in self.broker_to_users:
                self.new_user(user)
            self.broker_to_users.pop(broker)
            for service in self.broker_to_services:
                throughput = self.service_broker_throughput.pop((service, broker))
                # add this throughput to broker2 with high load on this service
                max_load = 0
                for b in self.brokers:
                    load = self.service_broker_load.get((service, b), 0)
                    if load > max_load:
                        broker2 = b
                        max_load = load
                new_throughput =\
                        self.service_broker_throughput[(service, broker2)] + throughput
                broker2.update_service(service, new_throughput)
                self.service_broker_throughput[(service, broker2)] = new_throughput
            self.broker_to_services.pop(broker)


    def fail(self):
        self.failed = True
        logging.debug(f'0, master, {self.id}, fail')

    def new_user(self, user):
        best_distance = float('inf')
        for b in self.brokers:
            distance = distance_time(b.location, user.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.add_user(user)
        self.user_to_broker[user] = broker
        self.broker_to_users[broker].append(user)
        user.set_broker(broker)
        logging.debug(f'0, master, {self.id}, new_user, {user.id}, {broker.id}')

    def new_service(self, service):
        best_distance = float('inf')
        for b in self.brokers:
            distance = distance_time(b.location, service.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.add_service(service, service.throughput)
        self.service_broker_throughput[(service, broker)] = service.throughput
        self.broker_to_services[broker].append(service)
        logging.debug(f'0, master, {self.id}, new_service, {service.id}, {broker.id}')

    def selection_report(self, broker, service_loads, unsatisfied_users):
        # This is master load balancing among brokers.
        if self.failed:
            return False

        # Step A: Move unsatisfied users to close & lightly loaded brokers
        for user in unsatisfied_users:
            best_score = float('inf')
            old_distance = distance_time(broker.location, user.location)
            for b, load in self.total_broker_load.items():
                new_distance = distance_time(b.load, user.location)
                b_score = load + (new_distance - old_distance) / old_distance
                if b_score < best_score:
                    best_score = b_score
                    broker2 = b
            broker.remove_user(user)
            broker2.add_user(user)

        # Step B: Take some load off the loaded services
        total_load = total_throughput = 0
        for i, (service, broker_throughput) in enumerate(broker.services):
            self.service_broker_load[(service, broker)] = service_loads[i]
            total_load += service_loads[i]
            total_throughput += broker_throughput
            # is the service loaded on this broker?
            if service_loads[i] == broker_throughput:
                # can I give more throughput to this broker?
                if service.throughput == broker_throughput:
                    continue
                # find broker2 with less load on this service
                best_difference = 0
                for b in self.brokers:
                    difference = self.service_broker_throughput.get((service, b), 0) -\
                            self.service_broker_load.get((service, b), 0)
                    if difference > best_difference:
                        broker2 = b
                        best_difference = difference
                # transfer 1 throughput from broker2 to broker
                broker2_throughput = self.service_broker_throughput[(service, broker2)]
                broker2.update_service(service, broker2_throughput - 1)
                broker.update_service(service, broker_throughput + 1)
                self.service_broker_throughput[(service, broker2)] = broker2_throughput - 1
                self.service_broker_throughput[(service, broker)] = broker_throughput + 1
                # assume broker is still loaded
                self.service_broker_load[(service, broker)] = broker_throughput + 1

        if total_throughput:
            self.total_broker_load[broker] = total_load / total_throughput
        logging.debug(f'0, master, {self.id}, selection_report, {broker.id}, {total_load}, {total_throughput}')
        return True

    def service_fail_report(self, broker, service):
        self.broker_to_services[broker].remove(service)
        self.service_broker_load.pop((service, broker))
        self.service_broker_throughput.pop((service, broker))
        logging.debug(f'0, master, {self.id}, service_fail_report, {broker.id}, {service.id}')
