import os

class Config:

    def __init__(self):
        self.bronto_api_key = os.environ.get('BRONTO_API_KEY')
        self.bronto_api_endpoint = os.environ.get('BRONTO_API_ENDPOINT', 'https://api.eu.bronto.io')
