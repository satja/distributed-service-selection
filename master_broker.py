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
        self.broker_to_services = defaultdict(set)
        self.service_broker_load = defaultdict(int)
        self.total_broker_load = defaultdict(int)
        self.failed = False
        self.id = get_uid()
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
                self.user_to_broker[user] = broker
                self.broker_to_users[broker].append(user)
            for service, throughput in broker.get_services():
                self.service_broker_throughput[(service, broker)] = throughput
                self.broker_to_services[broker].add(service)
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
            self.total_broker_load.pop(broker, None)
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
                print(self.service_broker_throughput[(service, broker2)])
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
        self.broker_to_services[broker].add(service)
        logging.info(f'0, master, {self.id}, new_service, {service.id}, {service.throughput}, {broker.id}')

    def selection_report(self, broker, service_loads, unsatisfied_users):
        # This is master load balancing among brokers.
        if self.failed:
            return False
        logging.debug(f'0, master, {self.id}, selection_report, {broker.id}')

        # Step A: Move unsatisfied users to close & lightly loaded brokers
        for user in unsatisfied_users:
            best_score = float('inf')
            old_distance = distance_time(broker.location, user.location)
            for b in self.brokers:
                if b == broker:
                    continue
                load = self.total_broker_load[b]
                new_distance = distance_time(b.location, user.location)
                b_score = load + (new_distance - old_distance) / old_distance
                if b_score < best_score:
                    best_score = b_score
                    broker2 = b
            broker.remove_user(user)
            broker2.add_user(user)
            user.set_broker(broker2)
            self.user_to_broker[user] = broker2
            self.broker_to_users[broker].remove(user)
            self.broker_to_users[broker2].append(user)
            logging.debug(f'0, master, {self.id}, unsatisfied_user, {user.id}, {broker.id}, {broker2.id}')

        # Step B: remove throughput from lightly loaded services on broker
        # and add more throughput to highly loaded services on broker
        total_load = total_throughput = 0
        for i, (service, broker_throughput) in enumerate(broker.services):
            self.service_broker_load[(service, broker)] = service_loads[i]
            total_load += service_loads[i]
            total_throughput += broker_throughput

            # is the service highly loaded on this broker?
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
                # transfer 1 throughput from broker2
                self.transfer_throughput(service, broker2, broker, 1)
                self.service_broker_load[(service, broker)] += 1

            # is the service lightly loaded on this broker?
            elif broker_throughput >= 10 and service_loads[i] <= broker_throughput // 2:
                # find broker2 with high load on this service
                best_difference = float('inf')
                for b in self.brokers:
                    if b == broker:
                        continue
                    difference = self.service_broker_throughput.get((service, b), 0) -\
                            self.service_broker_load.get((service, b), 0)
                    if difference < best_difference:
                        broker2 = b
                        best_difference = difference
                # transfer half throughput to broker2
                self.transfer_throughput(service, broker, broker2, broker_throughput // 2)
                total_throughput -= broker_throughput // 2

        self.total_broker_load[broker] = total_load / total_throughput\
                if total_throughput else 1
        logging.debug(f'0, master, {self.id}, selection_report_done, {broker.id}, {total_load}, {total_throughput}')
        return True

    def transfer_throughput(self, service, source_broker, dest_broker, thr):
        assert source_broker != dest_broker
        logging.info(f'0, master, {self.id}, transfer_throughput, {service.id}, {source_broker.id}, {dest_broker.id}, {thr}')
        source_thr = self.service_broker_throughput[(service, source_broker)] - thr
        dest_thr = self.service_broker_throughput[(service, dest_broker)] + thr
        source_broker.update_service(service, source_thr)
        dest_broker.update_service(service, dest_thr)
        self.service_broker_throughput[(service, source_broker)] = source_thr
        self.service_broker_throughput[(service, dest_broker)] = dest_thr
        if source_thr == 0:
            self.broker_to_services[source_broker].remove(service)
        self.broker_to_services[dest_broker].add(service)

    def service_fail_report(self, service):
        logging.info(f'0, master, {self.id}, service_fail_report, {service.id}')
        for broker in self.brokers:
            throughput = self.service_broker_throughput.pop((service, broker), None)
            if throughput:
                broker.update_service(service, 0)
                self.broker_to_services[broker].remove(service)
                self.service_broker_load.pop((service, broker), None)
