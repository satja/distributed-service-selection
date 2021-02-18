import sys
import os
from random import *
from math import log
import numpy as np
import logging
from collections import defaultdict
from time import time
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
from multiprocessing import Pool

from user import User
from broker import Broker
from master_broker import MasterBroker
from service import Service
import algorithms
import util

def random_service():
    location = util.random_location()
    reliability = 1 - 0.1 ** max(np.random.normal(2, .5), 1)
    computation_time = max(1, np.random.normal(75, 50))
    throughput = max(1, int(np.random.normal(10, 5)))
    cost = max(0, np.random.uniform(-1, 4))
    return Service(location, throughput, reliability, computation_time, cost)

def random_user():
    location = util.random_location()
    min_reliability = 1 - 0.1 ** max(np.random.normal(1.7, .5), 1)
    max_response_time = randint(400, 1500)
    return User(location, min_reliability, max_response_time)

class Simulation:
    def __init__(self, num_users, num_services, num_brokers, algorithm,
            balance_users, balance_services, random_seed=0, ms_per_step=250):
        self.random_seed = random_seed
        seed(random_seed)
        self.ms_per_step = ms_per_step
        self.inactive_users = []
        self.users = []
        self.services = []
        self.brokers = []
        self.inactive_users = num_users
        self.inactive_services = num_services
        self.inactive_brokers = num_brokers
        self.balancing = (balance_users, balance_services)
        self.master_broker = MasterBroker(util.random_location(), self.balancing)
        if algorithm == 0:
            self.algorithm = algorithms.random_selection
        elif algorithm == 1:
            self.algorithm = algorithms.round_robin_selection
        elif algorithm == 2:
            self.algorithm = algorithms.greedy_selection
        elif algorithm == 3:
            self.algorithm = algorithms.ap_selection
        elif algorithm == 4:
            self.algorithm = algorithms.tp_selection

    def run(self):
        steps = int(self.inactive_users * 1.5)
        qos = []
        unanswered_reqs = 0
        unsatisfied_rt = unsatisfied_rel = unsatisfied_both = 0
        total_cost = 0
        user_moves = service_moves = 0
        for t in range(steps):
            if t % 10 == 0:
                logging.info(f"{t} out of {steps} steps...")
            
            # If there is an inactive broker, it appears.
            if self.inactive_brokers:
                self.inactive_brokers -= 1
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker, self.algorithm))

            self.master_broker.check_for_failed_brokers()
            if randrange(100) == 0 and len(self.brokers) > 1:
                self.master_broker.fail()

            start = time()

            # Service selection
            self.brokers = [broker for broker in self.brokers if not broker.failed]
            self.services = [service for service in self.services if not service.failed]
            for broker in self.brokers:
                selection_time = t * self.ms_per_step
                results, unanswered, unsatisfied, cost, loads = broker.perform_selection(selection_time)
                qos.extend(results)
                unanswered_reqs += unanswered
                unsatisfied_rel += unsatisfied[0]
                unsatisfied_rt += unsatisfied[1]
                unsatisfied_both += unsatisfied[2]
                total_cost += cost
                total_load = defaultdict(int)
                for service, load in loads.items():
                    total_load[service] += load
                for service, load in total_load.items():
                    assert load <= service.throughput, (service.id, load, service.throughput)

            logging.debug(time() - start)

            # Master broker maybe changed
            if self.master_broker != self.brokers[0].master_broker:
                self.master_broker = self.brokers[0].master_broker
                self.brokers = self.master_broker.brokers[:]
                # add new regular broker
                location = util.random_location()
                self.brokers.append(Broker(location, self.master_broker, self.algorithm))
            assert all(broker.master_broker == self.master_broker
                    for broker in self.brokers), [b.master_broker for b in self.brokers]

            start = time()

            # Master load balancing
            moves = self.master_broker.balance_brokers()
            user_moves += moves[0]
            service_moves += moves[1]
            total_thr = sum(s.throughput for s in self.services)
            total_thr_alt = sum(broker.total_throughput() for broker in self.brokers)
            assert total_thr == total_thr_alt, f'{total_thr}, {total_thr_alt}'

            logging.debug(time() - start)
           
            # If there is an inactive service, it appears.
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
            if randrange(100) == 0 and len(self.brokers) > 1:
                choice(self.brokers).fail()
                self.inactive_brokers += 1
            if randrange(100) == 0 and len(self.services) > 1:
                choice(self.services).fail()
                self.inactive_services += 1

            # New requests
            for user in self.users:
                for num_reqs in range(randrange(3)):
                    req_time = t * self.ms_per_step + randrange(self.ms_per_step)
                    user.send_request(req_time)
            logging.info('')

        successful = len(qos) - unanswered_reqs - unsatisfied_rt\
                - unsatisfied_both - unsatisfied_rel

        print(self.random_seed, len(qos), len(self.brokers), self.algorithm, self.balancing)

        return len(qos), total_cost / len(qos),\
                successful / len(qos), unanswered_reqs / len(qos),\
                (unsatisfied_rt + unsatisfied_both) / len(qos),\
                (unsatisfied_rel + unsatisfied_both) / len(qos)
        '''
        print('total =', len(qos))
        print('cost =', total_cost)
        print('success_rate =', successful / len(qos))
        print('unanswered_rate =', unanswered_reqs / len(qos))
        print('unsatisfied_rel =', unsatisfied_rel / len(qos))
        print('unsatisfied_rt =', unsatisfied_rt / len(qos))
        print('unsatisfied_both =', unsatisfied_both / len(qos))
        print('user_moves, service_moves =', user_moves, service_moves)
        '''

def simulate(params):
    s = Simulation(*params)
    results = s.run()
    ret = s.algorithm.__name__.replace('_selection', '').replace("_", ' ').title().replace('Ap', 'AP').replace("Tp", "TP")
    if len(s.brokers) == 1:
        ret += ',single broker,'
    elif s.balancing[0] and s.balancing[1]:
        ret += ',user&service balancing,'
    elif s.balancing[0]:
        ret += ',user balancing,'
    elif s.balancing[1]:
        ret += ',service balancing,'
    else:
        ret += ',no balancing,'
    result = dict()
    for (i, name) in [(1, 'Cost'), (2, 'Successful reqs.'), (3, 'Failed reqs.'),
            (4, 'Violated RT reqs.'), (5, 'Violated reliability reqs.')]:
        result[name] = ret + str(results[i]) + '\n'
    #print(result)
    return result

if __name__ == '__main__':
    '''
    num_users = int(sys.argv[1])
    num_services = int(sys.argv[2])
    num_brokers = int(sys.argv[3])
    algorithm = int(sys.argv[4])
    balance_users = int(sys.argv[5])
    balance_services = int(sys.argv[6])
    s = Simulation(num_users, num_services, num_brokers,
            algorithm, balance_users, balance_services)
    s.run()
    '''
    for name in ('Cost', 'Successful reqs.', 'Failed reqs.',\
            'Violated RT reqs.', 'Violated reliability reqs.'):
        with open(name + '.txt', 'w') as f:
            f.write('')
    params = []
    for random_seed in range(30):
        num_users, num_services, num_brokers = 1000, 100, 10
        for algorithm in range(5):
            for num_brokers, balance_users, balance_services in [
                    (1, 0, 0), (num_brokers, 0, 0), (num_brokers, 1, 0),
                    (num_brokers, 0, 1), (num_brokers, 1, 1)]:
                params.append((num_users, num_services, num_brokers, algorithm,
                    balance_users, balance_services, random_seed))
    with Pool(min(20, os.cpu_count())) as p:
         results = p.map(simulate, params)
    for name in ('Cost', 'Successful reqs.', 'Failed reqs.',\
            'Violated RT reqs.', 'Violated reliability reqs.'):
        with open(name + '.txt', 'a') as f:
            for r in results:
                f.write(r[name])
