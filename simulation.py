from sys import argv
from random import *
from math import log
import numpy as np

from user import User
from broker import Broker
from master_broker import MasterBroker
from service import Service
import util

class Simulation:
    def __init__(self, num_users, num_services, num_brokers, ms_per_step=1000):
        self.ms_per_step = ms_per_step
        self.inactive_users = []
        self.users = []
        self.inactive_services = []
        self.services = []
        self.brokers = []
        self.inactive_brokers = num_brokers
        self.master_broker = MasterBroker(util.random_location())

        for _ in num_services:
            location = util.random_location()
            reliability = 1 - 0.1 ** max(np.random.normal(2, .5), 1)
            computation_time = max(0.001, np.random.normal(0.1, 0.05))
            throughput = randint(10, 100)
            cost = max(np.random.uniform(-1, 4))
            self.inactive_services.append(
                    Service(location, throughput, reliability, computation_time, cost))

        for _ in num_users:
            location = util.random_location()
            min_reliability = 1 - 0.1 ** max(np.random.normal(2, .5), 1)
            max_response_time = np.random.normal(0.5)
            self.inactive_users.append(
                    User(location, min_reliability, max_response_time))

    def run(self, steps=10**3):
        for t in range(steps):
            if t % 100 == 0:
                print(f"{t} out of {steps} steps...")
            
            # If there is an inactive broker, it appears.
            if self.inactive_brokers:
                self.inactive_brokers -= 1
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker))

            # Master broker potential fail
            if randrange(100) == 0:
                self.master_broker.fail()

            # Service selection
            qos = []
            self.brokers = [broker for broker in self.brokers if not broker.is_failed()]
            for broker in self.brokers:
                selection_time = t * self.ms_per_step
                results = broker.perform_selection(selection_time)
                qos.extend(results)

            # Master broker maybe changed
            self.master_broker = self.brokers[0].master_broker
            assert all(broker.master_broker == self.master_broker
                    for broker in self.brokers), [b.master_broker for f in self.brokers]
           
            # If there is an inactive service, it can appear.
            if self.inactive_service and randrange(2):
                service = self.inactive_services.pop()
                self.services.append(service)
                self.master_broker.new_service(service)
            
            # If there is an inactive user, it can appear.
            if self.inactive_users and randrange(2):
                user = self.inactive_users.pop()
                self.users.append(user)
                self.master_broker.new_user(user)

            # Potential service/user/broker fail
            if randrange(10) == 0:
                entity = choice(self.services + self.users + self.brokers)
                entity.fail()

            # New requests
            for user in self.users:
                for num_reqs in randrange(4):
                    req_time = t * self.ms_per_step + randrange(self.ms_per_step)
                    user.send_request(req_time)

if __name__ == '__main__':
    num_users = int(sys.argv[1])
    num_services = int(sys.argv[2])
    num_brokers = int(sys.argv[3])
    steps = int(sys.argv[4])
    s = Simulation(num_users, num_services, num_brokers)
    s.run(steps)
