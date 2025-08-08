import time

import zstd
import urllib.request
import logging
import os
import json

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
            'User-Agent': 'bronto-mcp',
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

    def search(self, timestamp_start: int, timestamp_end: int, log_ids: list[str], where='',
               _select=None, group_by_keys=None):
        if group_by_keys is None:
            group_by_keys = []
        if _select is None:
            _select = ['@raw']
        url_path = 'search'
        params = {
            'from_ts': timestamp_start,
            'to_ts': timestamp_end,
            'where': where,
            'select': _select,
            'group_by_keys': group_by_keys
        }
        url_params = ('?' + "&".join([urllib.parse.urlencode({'from': log_id}) for log_id in log_ids]) +
                      f'&from_ts={params.get("from_ts")}&to_ts={params.get("to_ts")}&'
                      + urllib.parse.urlencode({'where': params.get("where")}) + '&' +
                      "&".join([urllib.parse.urlencode({'select': sel}) for sel in params.get("select")]) + '&' +
                      "&".join([urllib.parse.urlencode({'groups': key}) for key in params.get("group_by_keys")])
                      )
        req_w_params = os.path.join(self.api_endpoint, url_path) + url_params
        request = urllib.request.Request(req_w_params, headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Search failed, status=%s, reason=%s',resp.status, resp.reason)
                result = json.loads(resp.read())
                return result
        except Exception as e:
            print(e)

    def search_post(self, timestamp_start: int, timestamp_end: int, log_ids: list[str], where='',
                     _select=None, group_by_keys=None):
        if group_by_keys is None:
            group_by_keys = []
        if _select is None:
            _select = ['@raw']
        url_path = 'search'
        params = {
            'from_ts': timestamp_start,
            'to_ts': timestamp_end,
            'where': where,
            'select': _select,
            'from': log_ids,
            'groups': group_by_keys,
            'num_of_slices': 10
        }
        req_w_params = os.path.join(self.api_endpoint, url_path)
        request = urllib.request.Request(req_w_params, method='POST', data=json.dumps(params).encode(),
                                         headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Search failed, status=%s, reason=%s',resp.status, resp.reason)
                result = json.loads(resp.read())
                return result
        except Exception as e:
            print(e)


    def get_stats(self, timestamp_start: int, timestamp_end: int, log_ids: list[str], _select=None):
        url_path = 'search'
        params = {
            'from': ':'.join(log_ids),
            'from_ts': timestamp_start,
            'to_ts': timestamp_end,
            'groups': ['service', 'versions'],
            'where': "'version inventory'"
        }
        if _select is not None:
            params.update({'select': _select})
        url_params = ('?' + urllib.parse.urlencode({'from': params.get("from")}) +
                      f'&from_ts={params.get("from_ts")}&to_ts={params.get("to_ts")}&'
                      + urllib.parse.urlencode({'select': params.get("select"), 'where': params.get("where")}) + '&' +
                      "&".join([urllib.parse.urlencode({'groups': group}) for group in params.get("groups")]))
        req_w_params = os.path.join(self.api_endpoint, url_path) + url_params
        request = urllib.request.Request(req_w_params, headers=self.headers)
        with urllib.request.urlopen(request) as resp:
            if resp.status != 200 and resp.status != 201:
                logger.error('Search failed, status=%s, reason=%s',resp.status, resp.reason)
            result = json.loads(resp.read()).get('result', [])
            return result

    def get_keys(self, log_id):
        url_path = f'top-keys?log_id={log_id}'
        request = urllib.request.Request(os.path.join(self.api_endpoint, url_path), headers=self.headers)
        with urllib.request.urlopen(request) as resp:
            if resp.status != 200 and resp.status != 201:
                logger.error('Keys retrieval failed, log_id=%s status=%s, reason=%s',log_id, resp.status,
                             resp.reason)
            body = json.loads(resp.read())
            keys = list(body.get(log_id, {}).keys())
            logging.info('keys=%s', keys)
            return keys
