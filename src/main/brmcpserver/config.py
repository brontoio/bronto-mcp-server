import os

class Config:

    def __init__(self):
        self.bronto_api_key = os.environ.get('BRONTO_API_KEY', '09871117-bbe6-44d4-9ea0-4270ad6fecd2.U9ueIDEpGbUVe8kVyVOupUQ0Fo8K958hvihuVTbVFhU=')
        self.bronto_api_endpoint = os.environ.get('BRONTO_API_ENDPOINT', 'https://api.eu.staging.bronto.io')
