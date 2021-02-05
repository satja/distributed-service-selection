class MasterBroker:
    def __init__(self, location):
        self.location = location
        self.brokers = []
        self.user_to_broker = dict()
        self.broker_to_users = dict()
        self.service_to_broker = dict()
        self.broker_to_services = dict()

    def new_user(self, user):
        # TODO: broker selection
        broker.add_user(user)

    def new_service(self, service):
        # TODO: broker selection / throughput distribution
        broker.add_service(service, reqs)

    def broker_selection_report(broker, load):
        # TODO: potential broker load balancing
        pass
