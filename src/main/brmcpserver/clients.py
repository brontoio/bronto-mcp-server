import time

import zstd
import urllib.request
import logging
import os
import json
from typing import List, Dict, Optional

from models import DatasetKey, LogEvent

logger = logging.getLogger()


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
               _select=None, group_by_keys=None) -> List[LogEvent]:
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
        url_params = url_params[:len(url_params) - 1] if url_params.endswith('&') else url_params
        req_w_params = os.path.join(self.api_endpoint, url_path) + url_params
        request = urllib.request.Request(req_w_params, headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Search failed, status=%s, reason=%s',resp.status, resp.reason)
                result = json.loads(resp.read())
                log_events: List[LogEvent] = []
                for event in result.get('events', []):
                    log_event = LogEvent(message=event['@raw'],
                                         attributes={'@status': event['@status'], '@time': event['@time']})
                    log_event.attributes.update(event['attributes'])
                    log_event.attributes.update(event['message_kvs'])
                    log_events.append(log_event)
                return log_events
        except Exception as e:
            raise e

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


    def get_recent_keys(self, log_id) -> Dict[str, List[str]]:
        now = int(time.time()) * 1000
        ten_minutes_ago = now - 10 * 60 * 1000
        log_events: List[LogEvent] = self.search(ten_minutes_ago, now, [log_id], _select = ['*', '@raw'])
        keys_and_values: Dict[str, List[str]] = {}
        for event in log_events:
            for key in event.attributes:
                if key in keys_and_values:
                    keys_and_values[key].append(event.attributes[key])
                else:
                    keys_and_values[key] = [event.attributes[key]]
        keys_and_unique_values = {key: list(set(keys_and_values[key])) for key in keys_and_values}
        return keys_and_unique_values

    def get_top_keys(self, log_id) -> Dict[str, List[str]]:
        url_path = f'top-keys?log_id={log_id}'
        request = urllib.request.Request(os.path.join(self.api_endpoint, url_path), headers=self.headers)
        with urllib.request.urlopen(request) as resp:
            if resp.status != 200 and resp.status != 201:
                logger.error('Keys retrieval failed, log_id=%s status=%s, reason=%s',log_id, resp.status,
                             resp.reason)
            body = json.loads(resp.read())
            keys_and_values = {}
            for key in body[log_id]:
                if key in keys_and_values:
                    keys_and_values[key].extend(body[log_id][key].get('values', {}).keys())
                else:
                    keys_and_values[key] = body[log_id][key].get('values', {}).keys()
            logging.info('keys_and_values=%s', keys_and_values)
            return {key: list(set(keys_and_values[key])) for key in keys_and_values}


    @staticmethod
    def _get_dataset_key(key_name: str, dataset_keys: List[DatasetKey]) -> Optional[DatasetKey]:
        for dataset_key in dataset_keys:
            if dataset_key.name == key_name:
                return dataset_key
        return None


    def get_keys(self, log_id) -> List[DatasetKey]:
        recent_keys = self.get_recent_keys(log_id)
        top_keys = self.get_top_keys(log_id)
        result = []
        processed_keys = set()
        for key in recent_keys:
            if key in processed_keys:
                dataset = BrontoClient._get_dataset_key(key, result)
                dataset.add_values(recent_keys[key])
            else:
                result.append(DatasetKey(name=key, values=recent_keys[key]))
        for key in top_keys:
            if key in processed_keys:
                dataset = BrontoClient._get_dataset_key(key, result)
                dataset.add_values(top_keys[key])
            else:
                result.append(DatasetKey(name=key, values=top_keys[key]))
        return result
