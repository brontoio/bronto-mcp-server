"""Saved search tools: list, create and run saved searches."""

import json
from typing import Dict, Optional

from bronto.client import BrontoClient
from pydantic import Field
from typing_extensions import Annotated


class SavedSearchTools:
    """Tools to list, create and run Bronto saved searches"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    @staticmethod
    def _as_list(value) -> list:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [p.strip() for p in value.split(',') if p.strip()]
        return []

    def get_saved_searches(
            self,
            query: Annotated[Optional[str], Field(default='', description='Optional case-insensitive text filter.')] = '',
            saved_search_id: Annotated[Optional[str], Field(default='', description='Fetch one saved search by id.')] = '',
            limit: Annotated[Optional[int], Field(
                default=None, ge=1, le=100,
                description='Optional maximum number of saved searches to return after filtering.')] = None,
    ) -> Annotated[Dict, Field(description='The saved search, or a filtered list of saved searches.')]:
        if saved_search_id:
            return self.bronto_client.get_saved_searches(saved_search_id)
        result = self.bronto_client.get_saved_searches()
        items = result.get('saved_searches', []) if isinstance(result, dict) else result
        items = items if isinstance(items, list) else []
        if query:
            q = query.lower()
            items = [s for s in items if q in json.dumps(s, default=str).lower()]
        total = len(items)
        if limit:
            items = items[:limit]
        return {'saved_searches': items, 'total': total, 'shown': len(items), 'truncated': len(items) < total}

    def create_saved_search(
            self,
            name: Annotated[str, Field(description='Name of the saved search.')],
            log_ids: Annotated[list[str], Field(description='Dataset IDs the search runs over.', min_length=1)],
            search_filter: Annotated[Optional[str], Field(default='', description='Optional SQL-style WHERE filter.')] = '',
            select: Annotated[Optional[str], Field(default='*,@raw', description='Fields or aggregate to return.')] = '*,@raw',
            time_range: Annotated[Optional[str], Field(default='', description='Optional relative window, e.g. "Last 1 hour".')] = '',
            dry_run: Annotated[bool, Field(
                default=False,
                description='Validate and return the resolved saved-search body without creating anything.')] = False,
    ) -> Annotated[Dict, Field(description='The created saved search, including its id. '
                                           'With dry_run, {dry_run: true, saved_search_body} and nothing is created.')]:
        # search_details is a string->string map (the API 400s on a list); the dataset
        # ids also go top-level as a list.
        details = {'from': ','.join(log_ids), 'select': select or '*,@raw'}
        if search_filter:
            details['where'] = search_filter
        if time_range:
            details['time_range'] = time_range
        body = {'name': name, 'log_ids': log_ids, 'search_details': details}
        if dry_run:
            return {'dry_run': True, 'saved_search_body': body}
        return {'saved_search': self.bronto_client.create_saved_search(body)}

    def run_saved_search(
            self,
            saved_search_id: Annotated[str, Field(description='Id of the saved search to run.')],
            time_range: Annotated[Optional[str], Field(default='', description='Optional window override, e.g. "Last 1 hour".')] = '',
            limit: Annotated[Optional[int], Field(default=None, description='Optional max rows/events to return.')] = None,
    ) -> Annotated[Dict, Field(description='The events (@raw, @time, @status, message_kvs), or grouped aggregates under totals/groups_series.')]:
        fetched = self.bronto_client.get_saved_searches(saved_search_id)
        saved = fetched.get('saved_search', fetched) if isinstance(fetched, dict) else fetched
        details = saved.get('search_details') if isinstance(saved, dict) else None
        if not isinstance(details, dict) or not details:
            raise ValueError(f"saved search '{saved_search_id}' has no search_details to run")
        body = {}
        # UI-created saved searches keep their datasets only in the top-level log_ids.
        from_ids = self._as_list(details.get('from') or details.get('logIds') or details.get('log_ids')
                                 or saved.get('log_ids'))
        if from_ids:
            body['from'] = from_ids
        if str(details.get('from_expr') or details.get('fromExpr') or '').strip():
            body['from_expr'] = str(details.get('from_expr') or details.get('fromExpr')).strip()
        if str(details.get('where') or '').strip():
            body['where'] = str(details['where']).strip()
        body['select'] = self._as_list(details.get('select')) or ['*', '@raw']
        groups = self._as_list(details.get('groups'))
        if groups:
            body['groups'] = groups
        override = time_range or str(details.get('time_range') or details.get('timeRange') or '').strip()
        if override:
            body['time_range'] = override
        if limit:
            body['limit'] = limit
        return self._shape_search_response(self.bronto_client.run_search(body))

    @staticmethod
    def _shape_search_response(resp) -> Dict:
        """Trim a raw /search response.

        The raw payload repeats every event under `result` and `events` with per-event
        metadata and context links; keep each event's @raw/@time/@status/message_kvs.
        Grouped/aggregate responses keep groups_series/totals.
        """
        if not isinstance(resp, dict):
            return resp
        out = {}
        if resp.get('groups_series') or resp.get('totals'):
            for key in ('groups_series', 'totals'):
                if resp.get(key):
                    out[key] = resp[key]
        else:
            out['events'] = [
                {k: e[k] for k in ('@raw', '@time', '@status', 'message_kvs') if k in e}
                for e in resp.get('events') or [] if isinstance(e, dict)
            ]
        for key in ('explain', 'links'):
            if resp.get(key):
                out[key] = resp[key]
        return out
