import urllib.request
import logging
import json
from typing import List, Dict, Optional
from urllib.error import HTTPError

from bronto.models import DatasetKey, LogEvent

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

    @staticmethod
    def _url_join(base, path):
        """Join URL parts using forward slashes (os.path.join uses backslashes on Windows)."""
        return base.rstrip('/') + '/' + path.lstrip('/')

    def get_datasets(self):
        url_path = 'logs'
        request = urllib.request.Request(BrontoClient._url_join(self.api_endpoint, url_path), headers=self.headers)
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
        req_w_params = BrontoClient._url_join(self.api_endpoint, url_path) + url_params
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
        req_w_params = BrontoClient._url_join(self.api_endpoint, url_path)
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

    def get_top_keys(self, log_id) -> Dict[str, List[str]]:
        url_path = f'top-keys?log_id={log_id}'
        request = urllib.request.Request(BrontoClient._url_join(self.api_endpoint, url_path), headers=self.headers)
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
                dataset_keys = body.get(log_id, {})  # absent when the dataset has no top keys yet
                for key in dataset_keys:
                    if key in keys_and_values:
                        keys_and_values[key].extend(dataset_keys[key].get('values', {}).keys())
                    else:
                        keys_and_values[key] = dataset_keys[key].get('values', {}).keys()
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

    def get_all_datasets_top_keys(self) -> Dict[str, List[str]]:
        url_path = f'top-keys'
        request = urllib.request.Request(BrontoClient._url_join(self.api_endpoint, url_path), headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Keys retrieval failed, status=%s, reason=%s',resp.status, resp.reason)
                    raise FailedBrontoRequestException(f'Cannot retrieve top keys from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    body = json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode search response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved data')

                log_ids_and_keys: Dict[str, List[str]]  = {}
                for log_id in body:
                    if log_id not in log_ids_and_keys:
                        log_ids_and_keys[log_id] = []
                    log_ids_and_keys[log_id].extend(body[log_id].keys())
                logging.info('log_ids_and_keys=%s', log_ids_and_keys)
                return log_ids_and_keys
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

    def get_all_datasets_top_keys_and_values(self) -> Dict[str, Dict[str, List[str]]]:
        url_path = f'top-keys'
        request = urllib.request.Request(BrontoClient._url_join(self.api_endpoint, url_path), headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status != 200 and resp.status != 201:
                    logger.error('Keys retrieval failed, status=%s, reason=%s',resp.status, resp.reason)
                    raise FailedBrontoRequestException(f'Cannot retrieve top keys from Bronto. status={resp.status}, '
                                                       f'reason="{resp.reason}"')
                try:
                    body = json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode search response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected format for retrieved data')

                log_ids_and_keys_and_values: Dict[str, Dict[str, List[str]]]  = {}
                for log_id in body:
                    if log_id not in log_ids_and_keys_and_values:
                        log_ids_and_keys_and_values[log_id] = {}
                    log_ids_and_keys_and_values[log_id].update({key: [value for value in body[log_id][key]['values'].keys()] for key in body[log_id]})
                logging.info('log_ids_and_keys_and_values=%s', log_ids_and_keys_and_values)
                return log_ids_and_keys_and_values
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
    def get_dataset_key(key_name: str, dataset_keys: List[DatasetKey]) -> Optional[DatasetKey]:
        for dataset_key in dataset_keys:
            if dataset_key.name == key_name:
                return dataset_key
        return None

    def get_keys(self, log_id) -> List[DatasetKey]:
        top_keys = self.get_top_keys(log_id)
        result = []
        processed_keys = set()
        for key in top_keys:
            if key in processed_keys:
                dataset = BrontoClient.get_dataset_key(key, result)
                dataset.add_values(top_keys[key])
            else:
                result.append(DatasetKey(name=key, values=top_keys[key]))
                processed_keys.add(key)
        return result

    @staticmethod
    def _error_details(error: HTTPError) -> str:
        """Bronto's own explanation from an error response body, if it has one."""
        try:
            details = json.loads(error.read()).get('details')
        except Exception:
            return ''
        return f' {details}' if details else ''

    def _request(self, method, url_path, params=None, body=None):
        """Generic Bronto request with the shared error mapping.

        GET query params or a JSON POST body. Returns the decoded JSON object.
        """
        url = BrontoClient._url_join(self.api_endpoint, url_path)
        if params:
            from urllib.parse import urlencode
            url = url + '?' + urlencode(params, doseq=True)
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(url, method=method, data=data, headers=self.headers)
        try:
            with urllib.request.urlopen(request) as resp:
                if resp.status not in (200, 201):
                    logger.error('Bronto request failed, status=%s, reason=%s', resp.status, resp.reason)
                    raise FailedBrontoRequestException(
                        f'Bronto request failed. status={resp.status}, reason="{resp.reason}"')
                try:
                    return json.loads(resp.read())
                except json.decoder.JSONDecodeError as _:
                    logger.error('Cannot decode Bronto response', exc_info=True)
                    raise BrontoResponseDecodingException('Unexpected Bronto response format')
        except (FailedBrontoRequestException, BrontoResponseDecodingException) as e:
            raise e
        except HTTPError as e:
            details = BrontoClient._error_details(e)
            if e.code == 400:
                raise BrontoResponseException('The request was rejected as malformed. Check the parameters.' + details)
            if e.code == 403:
                raise BrontoResponseException('Not allowed. Please check your Bronto API key permissions.' + details)
            if e.code == 401:
                raise BrontoResponseException('Not authorised. Please check your Bronto API key and endpoint.' + details)
            raise BrontoResponseException(f'Bronto request failed with status {e.code}.' + details)
        except Exception as _:
            logger.exception('Cannot interact with Bronto', exc_info=True)
            raise Exception('Cannot interact with Bronto. Please check endpoint configuration.')

    def get_users(self) -> List[Dict]:
        """Return the org's users (id + email), used to resolve a notification target."""
        body = self._request('GET', 'users')
        users = body.get('users', body) if isinstance(body, dict) else body
        return users if isinstance(users, list) else []

    def create_monitor(self, monitor_body: Dict) -> Dict:
        """Create a monitor via POST /monitors and return the created object."""
        return self._request('POST', 'monitors', body=monitor_body)

    def get_saved_searches(self, saved_search_id=None) -> Dict:
        if saved_search_id:
            return self._request('GET', f'saved-searches/{urllib.parse.quote(str(saved_search_id), safe="")}')
        return self._request('GET', 'saved-searches')

    def create_saved_search(self, body: Dict) -> Dict:
        return self._request('POST', 'saved-searches', body=body)

    def list_monitors(self) -> List[Dict]:
        body = self._request('GET', 'monitors')
        monitors = body.get('monitors', body) if isinstance(body, dict) else body
        return monitors if isinstance(monitors, list) else []

    def get_monitor_events(self, monitor_id: str) -> List[Dict]:
        body = self._request('GET', f'monitors/{urllib.parse.quote(str(monitor_id), safe="")}/events')
        events = body.get('events', body) if isinstance(body, dict) else body
        return events if isinstance(events, list) else []

    def list_metric_definitions(self) -> List[Dict]:
        body = self._request('GET', 'metrics/definitions')
        defs = body.get('definitions', body) if isinstance(body, dict) else body
        return defs if isinstance(defs, list) else []

    def get_error_analytics(self, time_range: str = 'Last 24 hours') -> Dict:
        return self._request('GET', 'analytics/errors', params={'time_range': time_range})

    def get_usage_by_log(self, usage_type: str, time_range: str = 'This month') -> List[Dict]:
        body = self._request('GET', 'usage/organizations/logs',
                             params={'usage_type': usage_type, 'delta': 'true',
                                     'limit': 500, 'time_range': time_range})
        groups = body.get('groups_series') if isinstance(body, dict) else None
        rows = []
        for g in groups or []:
            if isinstance(g, dict) and g.get('name') is not None:
                rows.append({'log_id': str(g['name']), 'bytes_total': int(g.get('value') or 0),
                             'count': int(g.get('count') or 0)})
        return rows

    def get_usage_limits(self) -> List[Dict]:
        body = self._request('GET', 'limits')
        limits = body.get('limits') if isinstance(body, dict) else None
        return limits if isinstance(limits, list) else []

    def run_search(self, body: Dict) -> Dict:
        """POST a raw /search body (used to re-run a saved search verbatim)."""
        return self._request('POST', 'search', body=body)
