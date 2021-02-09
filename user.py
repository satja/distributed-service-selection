class User:
    def __init__(self, location, min_reliability, max_response_time):
        self.location = location
        self.min_reliability = min_reliability
        self.max_response_time = max_response_time

    def send_request(self, request_time):
        self.broker.new_request(request_time);

    def set_broker(self, broker):
        self.broker = broker
