import time

import urllib.request
import logging
import os
import json
from typing import List, Dict, Optional
from urllib.error import HTTPError

from models import DatasetKey, LogEvent

logger = logging.getLogger()


class FailedBrontoRequestException(Exception):
    pass


class BrontoResponseDecodingException(Exception):
    pass


class BrontoResponseException(Exception):
    pass


class BrontoClient:

    def __init__(self, api_key, api_endpoint):
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'bronto-mcp',
            'x-bronto-api-key': self.api_key
        }

    def get_datasets(self):
        url_path = 'logs'
        request = urllib.request.Request(os.path.join(self.api_endpoint, url_path), headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Dataset retrieval failed, status=%s, reason=%s',resp.status, resp.reason)
                    raise FailedBrontoRequestException(f'Cannot retrieve datasets from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    datasets = json.loads(resp.read()).get('logs', [])
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode dataset retrieval response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved datasets')
                return datasets
        except (FailedBrontoRequestException, BrontoResponseDecodingException) as e:
            raise e
        except HTTPError as e:
            if e.code == 400:
                raise BrontoResponseException('One of the search parameters is unsuitable. Check the filter syntax as '
                                              'well as the names of the keys used in the "where", "_select" and '
                                              '"group_by_keys" parameters.')
            if e.code == 403:
                raise BrontoResponseException('You are not allowed to perform this Bronto search. Please check your '
                                              'Bronto API key')
            if e.code == 401:
                raise BrontoResponseException('You are not authorised to perform this Bronto search. Please check your '
                                              'Bronto API key, as well as the Bronto endpoint, to make sure that they '
                                              'match')
        except Exception as _:
            logger.exception('Cannot interact with Bronto', exc_info=True)
            raise Exception('Cannot interact with Bronto. Please check endpoint configuration.')

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
                    raise FailedBrontoRequestException(f'Cannot retrieve data from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    result = json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode search response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved data')

                log_events: List[LogEvent] = []
                for event in result.get('events', []):
                    log_event = LogEvent(message=event['@raw'],
                                         attributes={'@status': event['@status'], '@time': event['@time']})
                    log_event.attributes.update(event['attributes'])
                    log_event.attributes.update(event['message_kvs'])
                    log_events.append(log_event)
                return log_events
        except (FailedBrontoRequestException, BrontoResponseDecodingException) as e:
            raise e
        except HTTPError as e:
            if e.code == 400:
                raise BrontoResponseException('One of the search parameters is unsuitable. Check the filter syntax as '
                                              'well as the names of the keys used in the "where", "_select" and '
                                              '"group_by_keys" parameters.')
            if e.code == 403:
                raise BrontoResponseException('You are not allowed to perform this Bronto search. Please check your '
                                              'Bronto API key')
            if e.code == 401:
                raise BrontoResponseException('You are not authorised to perform this Bronto search. Please check your '
                                              'Bronto API key, as well as the Bronto endpoint, to make sure that they '
                                              'match')
        except Exception as _:
            logger.exception('Cannot interact with Bronto', exc_info=True)
            raise Exception('Cannot interact with Bronto. Please check endpoint configuration.')

    def search_post(self, timestamp_start: int, timestamp_end: int, log_ids: list[str], where='', _select=None,
                    group_by_keys=None):
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
                    raise FailedBrontoRequestException(f'Cannot retrieve data from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    result = json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode search response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved data')
                return result
        except (FailedBrontoRequestException, BrontoResponseDecodingException) as e:
            raise e
        except HTTPError as e:
            if e.code == 400:
                raise BrontoResponseException('One of the search parameters is unsuitable. Check the filter syntax as '
                                              'well as the names of the keys used in the "where", "_select" and '
                                              '"group_by_keys" parameters.')
            if e.code == 403:
                raise BrontoResponseException('You are not allowed to perform this Bronto search. Please check your '
                                              'Bronto API key')
            if e.code == 401:
                raise BrontoResponseException('You are not authorised to perform this Bronto search. Please check your '
                                              'Bronto API key, as well as the Bronto endpoint, to make sure that they '
                                              'match')
        except Exception as _:
            logger.error('Cannot interact with Bronto', exc_info=True)
            raise Exception('Cannot interact with Bronto. Please check endpoint configuration.')

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
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Keys retrieval failed, log_id=%s status=%s, reason=%s',log_id, resp.status,
                                 resp.reason)
                    raise FailedBrontoRequestException(f'Cannot retrieve top keys from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    body = json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode search response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved data')

                keys_and_values = {}
                for key in body[log_id]:
                    if key in keys_and_values:
                        keys_and_values[key].extend(body[log_id][key].get('values', {}).keys())
                    else:
                        keys_and_values[key] = body[log_id][key].get('values', {}).keys()
                logging.info('keys_and_values=%s', keys_and_values)
                return {key: list(set(keys_and_values[key])) for key in keys_and_values}
        except (FailedBrontoRequestException, BrontoResponseDecodingException) as e:
            raise e
        except HTTPError as e:
            if e.code == 400:
                raise BrontoResponseException('One of the search parameters is unsuitable. Check the filter syntax as '
                                              'well as the names of the keys used in the "where", "_select" and '
                                              '"group_by_keys" parameters.')
            if e.code == 403:
                raise BrontoResponseException('You are not allowed to perform this Bronto search. Please check your '
                                              'Bronto API key')
            if e.code == 401:
                raise BrontoResponseException('You are not authorised to perform this Bronto search. Please check your '
                                              'Bronto API key, as well as the Bronto endpoint, to make sure that they '
                                              'match')
        except Exception as _:
            logger.error('Cannot interact with Bronto', exc_info=True)
            raise Exception('Cannot interact with Bronto. Please check endpoint configuration.')

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
