import sys
from random import *
from math import log
import numpy as np
import logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

from user import User
from broker import Broker
from master_broker import MasterBroker
from service import Service
import util

def random_service():
    location = util.random_location()
    reliability = 1 - 0.1 ** max(np.random.normal(2, .5), 1)
    computation_time = max(1, np.random.normal(100, 50))
    throughput = randint(1, 50)
    cost = max(0, np.random.uniform(-1, 4))
    return Service(location, throughput, reliability, computation_time, cost)

def random_user():
    location = util.random_location()
    min_reliability = 1 - 0.1 ** max(np.random.normal(1.5, .5), 1)
    max_response_time = randint(250, 1000)
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
        steps = self.inactive_users * 3
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

            # Service selection
            self.brokers = [broker for broker in self.brokers if not broker.failed]
            self.services = [service for service in self.services if not service.failed]
            for broker in self.brokers:
                selection_time = t * self.ms_per_step
                results, unsatisfied = broker.perform_selection(selection_time)
                qos.extend(results)
                total_unsatisfied_reqs += unsatisfied

            # Master broker maybe changed
            if self.master_broker != self.brokers[0].master_broker:
                self.master_broker = self.brokers[0].master_broker
                self.brokers = self.master_broker.brokers[:]
                # add new regular broker
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker))
            assert all(broker.master_broker == self.master_broker
                    for broker in self.brokers), [b.master_broker for b in self.brokers]

            total_thr = sum(s.throughput for s in self.services)
            total_thr_alt = sum(sum([thr for s, thr in broker.services])\
                    for broker in self.brokers)
            assert total_thr == total_thr_alt, f'{total_thr}, {total_thr_alt}'
           
            # If there is an inactive service, it can appear.
            if self.inactive_services and randrange(2):
                service = random_service()
                self.services.append(service)
                self.master_broker.new_service(service)
                self.inactive_services -= 1
            
            # If there is an inactive user, it can appear.
            if self.inactive_users and randrange(2):
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

        print(f'{total_unsatisfied_reqs} out of {len(qos)} reqs unsatisfied')

if __name__ == '__main__':
    num_users = int(sys.argv[1])
    num_services = int(sys.argv[2])
    num_brokers = int(sys.argv[3])
    s = Simulation(num_users, num_services, num_brokers)
    s.run()
