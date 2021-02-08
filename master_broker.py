import random
from collections import defaultdict

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

    def move_user(self, user, broker):
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

    def check_for_failed_brokers(self):
        failed_brokers = [broker in self.brokers if broker.is_failed()]
        self.brokers = [broker in self.brokers if not broker.is_failed()]
        for broker in failed_brokers:
            self.total_broker_load.pop(broker)
            for user in self.broker_to_users:
                self.new_user(user)
            self.broker_to_users.pop(broker)
            for service in self.broker_to_services:
                throughput = self.service_broker_throughput.pop((service, broker))
                # TODO: find new broker for service 
            self.broker_to_services.pop(broker)

    def fail(self):
        self.failed = True

    def new_user(self, user):
        best_distance = float('inf')
        for b in brokers:
            distance = distance_time(b.location, user.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.add_user(user)
        self.user_to_broker[user] = broker
        self.broker_to_users[broker].append(user)

    def new_service(self, service):
        best_distance = float('inf')
        for b in brokers:
            distance = distance_time(b.location, service.location)
            if distance < best_distance:
                broker = b
                best_distance = distance
        broker.add_service(service, service.throughput)
        self.service_broker_throughput[(service, broker)] = service.throughput
        self.broker_to_services[broker].append((service, service.throughput))

    def broker_selection_report(self, broker, service_loads, unsatisfied_users):
        # This is master load balancing among brokers.
        if self.failed:
            return False

        # Step A: Move unsatisfied users to close & lightly loaded brokers
        for user in unsatisfied_users:
            self.move_user(user, broker)

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
                for b in brokers:
                    difference = self.service_broker_throughput[(service, b)] -\
                            self.service_broker_load[(service, b)]
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
        self.total_broker_load[broker] = total_load / total_throughput
        return True

    def service_fail_report(self, broker, service):
        self.service_broker_throughput.pop(service)
        self.broker_to_services = broker.services[:]
        self.service_broker_load.pop((service, broker))
