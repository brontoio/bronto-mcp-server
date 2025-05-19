import time

import zstd
import urllib.request
import logging
import os
import json

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
handler = logging.FileHandler('/tmp/bronto_mcp.log')
handler.setLevel(logging.DEBUG)

# Define the log message format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Attach the handler to the logger
logger.addHandler(handler)

class BrontoClient:

    def __init__(self, api_key, api_endpoint):
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'bronto-behaviour-comparison',
            'x-bronto-api-key': self.api_key
        }

    def _create_export(self, timestamp_start: int, timestamp_end: int, log_ids: list[str]):
        url_path = 'exports'
        payload = json.dumps({'search_details':
                                  {'from': ':'.join(log_ids), 'from_ts': timestamp_start, 'to_ts': timestamp_end,
                                   'where': ''}
                              }).encode()
        endpoint = os.path.join(self.api_endpoint, url_path)
        request = urllib.request.Request(endpoint, data=payload, headers=self.headers)
        logger.info('Export creation. endpoint=%s, path=%s, payload=%s', endpoint, url_path, payload)
        try:
            with urllib.request.urlopen(request) as resp:
                logger.info('Export creation status, status=%s',resp.status)
                if resp.status != 200 and resp.status != 201:
                    logger.error('Export creation failed, status=%s, reason=%s',resp.status, resp.reason)
                    return None
                result = json.loads(resp.read())
                export_id = result.get('export_id')
                logger.info('Export created successfully. export_id=%s', export_id)
                return result
        except Exception as e:
            logger.error('ERROR creating export! endpoint=%s, path=%s, payload=%s', endpoint, url_path,
                         payload, exc_info=e)

    def retrieve_log_data(self, export_id, progress):
        url_path = f'exports/{export_id}'
        request = urllib.request.Request(os.path.join(self.api_endpoint, url_path), headers=self.headers)
        max_attempts = 5
        attempts = 0
        wait_between_attempts_sec = 3
        result = {}
        while progress < 100 and attempts < max_attempts:
            time.sleep(wait_between_attempts_sec)
            attempts += 1
            with urllib.request.urlopen(request) as resp:
                logger.info('Export retrieval status, status=%s',resp.status)
                if resp.status != 200 and resp.status != 201:
                    logger.error('Export retrieval failed, status=%s, reason=%s',resp.status, resp.reason)
                    continue
                result = json.loads(resp.read())
                progress = result.get('progress')
                logger.info('Export retrieval progress, progress=%s',progress)
        if progress == 100:
            logger.info('Export completed successfully. export_id=%s', export_id)
            s3_location = result.get('location')
            with urllib.request.urlopen(s3_location) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Data retrieval from S3 failed, status=%s, reason=%s, url=%s',resp.status,
                                 resp.reason, s3_location)
                    return
                return resp.read()
        return None

    def get_log_data(self, timestamp_start: int, timestamp_end: int, log_ids: list[str]):
        result = self._create_export(timestamp_start, timestamp_end, log_ids)
        if result is None:
            return None
        export_id = result.get('export_id')
        progress = result.get('progress')
        compressed_data = self.retrieve_log_data(export_id, progress)
        raw_data = zstd.decompress(compressed_data)
        data = raw_data.decode()
        return data

    def get_datasets(self):
        url_path = 'logs'
        request = urllib.request.Request(os.path.join(self.api_endpoint, url_path), headers=self.headers)
        with urllib.request.urlopen(request) as resp:
            if resp.status != 200 and resp.status != 201:
                logger.error('Dataset retrieval failed, status=%s, reason=%s',resp.status, resp.reason)
            datasets = json.loads(resp.read()).get('logs', [])
            logging.info('DATASETS=%s', datasets)
            return datasets
