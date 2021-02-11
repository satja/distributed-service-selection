import sys
from random import *
from math import log
import numpy as np
import logging
from collections import defaultdict
from time import time
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

from user import User
from broker import Broker
from master_broker import MasterBroker
from service import Service
import util

def random_service():
    location = util.random_location()
    reliability = 1 - 0.1 ** max(np.random.normal(2, .5), 1)
    computation_time = max(1, np.random.normal(75, 50))
    throughput = max(1, int(np.random.normal(30, 20)))
    cost = max(0, np.random.uniform(-1, 4))
    return Service(location, throughput, reliability, computation_time, cost)

def random_user():
    location = util.random_location()
    min_reliability = 1 - 0.1 ** max(np.random.normal(1.5, .5), 1)
    max_response_time = randint(300, 1000)
    return User(location, min_reliability, max_response_time)

class Simulation:
    def __init__(self, num_users, num_services, num_brokers, ms_per_step=250):
        self.ms_per_step = ms_per_step
        self.inactive_users = []
        self.users = []
        self.services = []
        self.brokers = []
        self.inactive_users = num_users
        self.inactive_services = num_services
        self.inactive_brokers = num_brokers
        self.master_broker = MasterBroker(util.random_location())

    def run(self):
        steps = int(self.inactive_users * 1.5)
        qos = []
        total_unsatisfied_reqs = 0
        for t in range(steps):
            if t % 10 == 0:
                logging.info(f"{t} out of {steps} steps...")
            
            # If there is an inactive broker, it appears.
            if self.inactive_brokers:
                self.inactive_brokers -= 1
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker))

            self.master_broker.check_for_failed_brokers()
            if randrange(100) == 0:
                self.master_broker.fail()

            start = time()

            # Service selection
            self.brokers = [broker for broker in self.brokers if not broker.failed]
            self.services = [service for service in self.services if not service.failed]
            for broker in self.brokers:
                selection_time = t * self.ms_per_step
                results, unsatisfied, loads = broker.perform_selection(selection_time)
                qos.extend(results)
                total_unsatisfied_reqs += unsatisfied
                total_load = defaultdict(int)
                for service, load in loads.items():
                    total_load[service] += load
                for service, load in total_load.items():
                    assert load <= service.throughput, (service.id, load, service.throughput)

            logging.info(time() - start)

            # Master broker maybe changed
            if self.master_broker != self.brokers[0].master_broker:
                self.master_broker = self.brokers[0].master_broker
                self.brokers = self.master_broker.brokers[:]
                # add new regular broker
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker))
            assert all(broker.master_broker == self.master_broker
                    for broker in self.brokers), [b.master_broker for b in self.brokers]

            start = time()

            # Master load balancing
            self.master_broker.balance_brokers()
            total_thr = sum(s.throughput for s in self.services)
            total_thr_alt = sum(sum([thr for s, thr in broker.services])\
                    for broker in self.brokers)
            assert total_thr == total_thr_alt, f'{total_thr}, {total_thr_alt}'

            logging.info(time() - start)
           
            # If there is an inactive service, appears.
            if self.inactive_services:
                service = random_service()
                self.services.append(service)
                self.master_broker.new_service(service)
                self.inactive_services -= 1
            
            # If there is an inactive user, it appears.
            if self.inactive_users:
                user = random_user()
                self.users.append(user)
                self.master_broker.new_user(user)
                self.inactive_users -= 1

            # Potential service/broker fail
            if randrange(100) == 0 and len(self.brokers):
                choice(self.brokers).fail()
                self.inactive_brokers += 1
            if randrange(100) == 0 and len(self.services):
                service = choice(self.services)
                service.fail()
                self.inactive_services += 1

            # New requests
            for user in self.users:
                for num_reqs in range(randrange(4)):
                    req_time = t * self.ms_per_step + randrange(self.ms_per_step)
                    user.send_request(req_time)
            logging.info('')

        print(f'{total_unsatisfied_reqs} out of {len(qos)} reqs unsatisfied')

if __name__ == '__main__':
    num_users = int(sys.argv[1])
    num_services = int(sys.argv[2])
    num_brokers = int(sys.argv[3])
    s = Simulation(num_users, num_services, num_brokers)
    s.run()
