class Broker:
    def __init__(self, location, master_broker):
        self.location = location
        self.users = []
        self.services = []
        self.requests = []
        self.master_broker = master_broker

    def add_user(self, user):
        self.users.append(user)

    def remove_user(self, user):
        self.users.remove(user)

    def add_service(self, service, reqs):
        self.services.append((service, reqs))

    def update_service(self, service, reqs):
        for i, (s, r) in enumerate(self.services):
            if s == service:
                if reqs == 0:
                    self.services.pop(i)
                else:
                    self.services[i] = (service, reqs)
                return

    def new_request(self, user, request_time):
        self.requests.append((user, request_time))

    def perform_selection(self, begin_time):
        # TODO: distribute self.requests among self.services
        self.requests = []
        selection_done_time = begin_time + 0.5

        # TODO: notify master

        # TODO: notify services
        qos = []
        for request, service in results:
            qos.append(service.serve_request(request[0], request[1], selection_done_time)
        return qos
