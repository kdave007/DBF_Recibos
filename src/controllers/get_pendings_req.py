from src.utils.get_enc import EncEnv

class GetPendingReq:

    def __init__(self):
        self.env = EncEnv()
        self.base_endpoint = self.env.get("API_BASE_URL")# THIS MUST BE A NEW URL
        self.api = self.env.get("API_KEY")

    def send(self, pending_records):
        pass