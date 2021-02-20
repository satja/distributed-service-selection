from util import *
import logging

class User:
    def __init__(self, location, min_reliability, max_response_time, premium):
        self.location = location
        self.min_reliability = min_reliability
        self.max_response_time = max_response_time
        self.id = get_uid()
        self.premium = premium
        logging.debug(f'0, user, {self.id}, __init__, {location}, {min_reliability}, {max_response_time}')

    def send_request(self, request_time):
        logging.debug(f'{request_time}, user, {self.id}, send_request')
        self.broker.new_request(self, request_time);

    def set_broker(self, broker):
        self.broker = broker
        logging.debug(f'0, user, {self.id}, set_broker, {broker.id}')

    def get_broker(self):
        return self.broker
