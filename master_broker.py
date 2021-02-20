import random
from collections import defaultdict
import logging
from util import *

class MasterBroker:
    def __init__(self, location, balancing=(True, True)):
        self.location = location
        self.brokers = []
        self.broker_to_users = defaultdict(set)
        self.service_broker_throughput = defaultdict(int)
        self.broker_to_services = defaultdict(set)
        self.service_broker_load = defaultdict(int)
        self.broker_free_space = defaultdict(int)
        self.failed = False
        self.id = get_uid()
        self.unsatisfied_users = []
        self.light_load = []
        self.high_load = []
        self.services = set()
        self.balancing = balancing
        logging.debug(f'0, master, {self.id}, __init__, {location}')

    def new_broker(self, broker):
        logging.debug(f'0, master, {self.id}, new_broker, {broker.id}')
        self.brokers.append(broker)
        for broker in self.brokers:
            broker.update_brokers_list(self.brokers)

    def fill_brokers_data(self, brokers):
        logging.info(f'0, master, {self.id}, fill_brokers_data')
        # this happens when a new master is elected among brokers
        for broker in brokers:
            broker.update_master(self)
            self.brokers.append(broker)
            for user in broker.get_users():
                self.broker_to_users[broker].add(user)
            for service, throughput in broker.get_services():
                self.service_broker_throughput[(service, broker)] = throughput
                self.broker_to_services[broker].add(service)
                self.services.add(service)
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
            self.broker_free_space.pop(broker, None)
            for user in self.broker_to_users[broker]:
                self.new_user(user)
            self.broker_to_users.pop(broker)
            logging.info(f'0, master, {self.id}, failed_broker, {broker.id}')
            for service in self.broker_to_services[broker]:
                old_distance = distance_time(broker.location, service.location)
                throughput = self.service_broker_throughput.pop((service, broker))
                # add this throughput to broker2 with high load
                # or small distance from this service
                max_score = -float('inf')
                for b in self.brokers:
                    load_score = self.service_broker_load.get((service, b), 0)
                    distance_score = distance_time(service.location, b.location) / old_distance
                    score = load_score - distance_score
                    if score > max_score:
                        broker2 = b
                        max_score = score
                new_throughput =\
                        self.service_broker_throughput[(service, broker2)] + throughput
                broker2.update_service(service, new_throughput)
                self.service_broker_throughput[(service, broker2)] = new_throughput
                self.broker_to_services[broker2].add(service)
                logging.info(f'0, master, {self.id}, move_service, {service.id}, {throughput}, {broker.id}, {broker2.id}')
            self.broker_to_services.pop(broker)

    def fail(self):
        self.failed = True
        logging.info(f'0, master, {self.id}, fail')

    def new_user(self, user):
        best_distance = float('inf')
        for b in self.brokers:
            distance = distance_time(b.location, user.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.add_user(user)
        self.broker_to_users[broker].add(user)
        user.set_broker(broker)
        logging.debug(f'0, master, {self.id}, new_user, {user.id}, {broker.id}')

    def new_service(self, service):
        best_distance = float('inf')
        for b in self.brokers:
            distance = distance_time(b.location, service.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.update_service(service, service.throughput)
        self.service_broker_throughput[(service, broker)] = service.throughput
        self.broker_to_services[broker].add(service)
        self.services.add(service)
        logging.info(f'0, master, {self.id}, new_service, {service.id}, {service.throughput}, {broker.id}')

    def selection_report(self, broker, service_loads,\
            unsatisfied_users, unanswered_reqs):
        if self.failed:
            return False
        total_load = total_throughput = 0
        for service, load in service_loads.items():
            self.service_broker_load[(service, broker)] = load
            total_load += load
            thr = self.service_broker_throughput[(service, broker)]
            total_throughput += thr
            self.light_load.extend([(service, broker)] * ((thr - load) // TRANSFER_UNIT))
        logging.info(f'{broker.id}, {len(broker.services)}, {total_throughput}, {len(self.services)}')
        self.broker_free_space[broker] = total_throughput - total_load
        self.high_load.extend([broker] * (unanswered_reqs // TRANSFER_UNIT))
        self.unsatisfied_users.extend([(user, broker) for user in unsatisfied_users])
        logging.debug(f'0, master, {self.id}, selection_report, {broker.id}, {total_load}, {total_throughput}')
        return True

    def balance_users(self):
        if not self.balancing[0]:
            return 0
        # Move unsatisfied users to close & lightly loaded brokers
        user_moves = 0
        light_brokers = sorted([(space, broker.id, broker) for broker, space in\
                self.broker_free_space.items()], reverse=True)[:len(self.brokers) // 5]
        for user, broker in self.unsatisfied_users:
            if not light_brokers:
                break
            max_score = -float('inf')
            max_space = light_brokers[0][0]
            old_distance = distance_time(broker.location, user.location)
            for space, _, b in light_brokers:
                if b == broker:
                    continue
                new_distance = distance_time(b.location, user.location)
                distance_score = sigmoid((new_distance - old_distance) / old_distance)
                space_score = space / max(max_space, 1)
                b_score = space_score - distance_score
                if b_score > max_score:
                    max_score = b_score
                    broker2 = b
            if max_score == -float('inf'):
                continue
            broker.remove_user(user)
            broker2.add_user(user)
            user.set_broker(broker2)
            self.broker_to_users[broker].remove(user)
            self.broker_to_users[broker2].add(user)
            logging.debug(f'0, master, {self.id}, unsatisfied_user, {user.id}, {broker.id}, {broker2.id}')
            user_moves += 1
        self.unsatisfied_users.clear()
        return user_moves

    def balance_services(self):
        if not self.balancing[1]:
            return 0
        # Move throughput from brokers with light load to brokers with high load
        service_moves = 0
        while self.light_load and self.high_load:
            broker2 = self.high_load.pop()
            distance, i, service, broker = min(
                    (distance_time(broker2.location, service.location), i, service, broker)
                    for i, (service, broker) in enumerate(self.light_load))
            self.light_load.remove((service, broker))
            source_thr = self.service_broker_throughput[(service, broker)] - TRANSFER_UNIT
            dest_thr = self.service_broker_throughput[(service, broker2)] + TRANSFER_UNIT
            logging.info(f'0, master, {self.id}, transfer_throughput, {service.id}, {broker.id}, {broker2.id}')
            broker.update_service(service, source_thr)
            broker2.update_service(service, dest_thr)
            self.service_broker_throughput[(service, broker)] = source_thr
            self.service_broker_throughput[(service, broker2)] = dest_thr
            if source_thr == 0:
                self.broker_to_services[broker].remove(service)
            self.broker_to_services[broker2].add(service)
            service_moves += 1
        self.light_load.clear()
        self.high_load.clear()
        return service_moves

    def balance_brokers(self, parity):
        logging.debug(f'0, master, {self.id}, balance_brokers')
        if self.balancing[0] and self.balancing[1]:
            if parity:
                return self.balance_users(), 0
            else:
                return 0, self.balance_services()
        return self.balance_users(), self.balance_services()

    def service_fail_report(self, service):
        logging.info(f'0, master, {self.id}, service_fail_report, {service.id}')
        self.services.discard(service)
        for broker in self.brokers:
            throughput = self.service_broker_throughput.pop((service, broker), None)
            if throughput:
                broker.update_service(service, 0)
                self.broker_to_services[broker].remove(service)
                self.service_broker_load.pop((service, broker), None)
