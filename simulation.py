from sys import argv
from random import *
from math import log
import numpy as np

from user import User
from broker import Broker
from master_broker import MasterBroker
from service import Service
from util impot *

class Simulation:
    def __init__(self, num_users, num_services, num_brokers):
        self.inactive_users = []
        self.active_users = []
        self.inactive_services = []
        self.active_services = []
        self.brokers = []
        master_broker = MasterBroker(util.random_location())

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

        for _ in num_brokers:
            location = util.random_location()
            self.brokers.append(Broker(location, master_broker))

    def run(self, steps=10**3):
        for t in range(steps):
            if t % 100 == 0:
                print(f"{t} out of {steps} steps...")

            qos = []
            for broker in self.brokers:
                qos.extend(broker.perform_selection(t))
           
            # If there is an inactive service, it can appear.
            if self.inactive_service and randrange(2):
                service = self.inactive_services.pop()
                self.active_services.append(service)
                self.master_broker.new_service(service)
            
            # If there is an inactive user, it can appear.
            if self.inactive_users and randrange(2):
                user = self.inactive_users.pop()
                self.active_users.append(user)
                self.master_broker.new_user(user)


if __name__ == '__main__':
    num_users = int(sys.argv[1])
    num_services = int(sys.argv[2])
    num_brokers = int(sys.argv[3])
    s = Simulation(num_users, num_services, num_brokers)
    s.run()
