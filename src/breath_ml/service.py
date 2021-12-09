from breath_api_interface.proxy import ServiceProxy
from breath_api_interface.queue import Queue
from breath_api_interface.service_interface import Service

class Prediction(Service):
    def __init__(self, proxy: ServiceProxy, request_queue: Queue, global_response_queue: Queue):
        super().__init__(proxy, request_queue, global_response_queue, "Prediction")

        