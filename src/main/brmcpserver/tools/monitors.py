"""Monitor tools: create, search and health-check alerting monitors."""

import json
import logging
from typing import Dict, Literal, Optional

from bronto.client import (
    BrontoClient,
    BrontoResponseDecodingException,
    BrontoResponseException,
    FailedBrontoRequestException,
)
from pydantic import Field
from typing_extensions import Annotated

logger = logging.getLogger()


class MonitorTools:
    """Tools to create, list and validate Bronto monitors"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    def create_monitor(
            self,
            name: Annotated[str, Field(description='The monitor name.')],
            notification_email: Annotated[str, Field(
                description='Email address to notify when the monitor fires. MUST be the email of an '
                            'existing Bronto user — ask the user for it if you do not have it.')],
            threshold: Annotated[float, Field(description='The alarm threshold for the monitor.')],
            comparison_operator: Annotated[
                Literal['ABOVE', 'BELOW', 'ABOVE_OR_EQUAL', 'BELOW_OR_EQUAL'],
                Field(description='How the metric is compared to the threshold.')],
            window: Annotated[str, Field(description='Evaluation window, e.g. "Last 15 minutes".')],
            log_ids: Annotated[list[str], Field(
                description='Dataset IDs the monitor evaluates over.', min_length=1)],
            select: Annotated[str, Field(
                default='count(*)',
                description='Aggregate the monitor evaluates, e.g. count(*) or avg(duration_ms).')] = 'count(*)',
            search_filter: Annotated[str, Field(
                default='', description='Optional SQL-style WHERE filter applied before aggregation.')] = '',
            dry_run: Annotated[bool, Field(
                default=False,
                description='Validate and return the resolved monitor body without creating anything.')] = False,
    ) -> Annotated[Dict, Field(description='The created monitor, including its id. Created muted for review. '
                                           'With dry_run, {dry_run: true, monitor_body} and nothing is created.')]:
        # Notifications need an existing user's email (no signed-in principal on the local server).
        emails = {str(u.get('email', '')).strip().lower()
                  for u in self.bronto_client.get_users() if u.get('email')}
        if notification_email.strip().lower() not in emails:
            raise ValueError(
                f"notification_email '{notification_email}' is not an existing Bronto user. "
                "Ask the user for a valid account email.")
        query = {'name': name, 'select': [select], 'from': log_ids}
        if search_filter:
            query['where'] = search_filter
        monitor_body = {
            'name': name,
            'monitor_type': 'PATTERN',
            'type': 'PATTERN',
            'threshold': threshold,
            'comparison_operator': comparison_operator,
            'window': window,
            'queries': [query],
            'mute_until': -1,  # always created muted for review
            'actions': [{'type': 'EMAIL', 'email': notification_email}],
        }
        if dry_run:
            return {'dry_run': True, 'monitor_body': monitor_body}
        logger.info('creating monitor name=%s log_ids=%s', name, log_ids)
        return self.bronto_client.create_monitor(monitor_body)

    def search_monitors(
            self,
            query: Annotated[Optional[str], Field(default='', description='Optional case-insensitive text filter.')] = '',
            status: Annotated[Optional[str], Field(default='', description='Optional exact status filter, e.g. OK, ALARM.')] = '',
            monitor_id: Annotated[Optional[str], Field(default='', description='Inspect one monitor and its recent events.')] = '',
    ) -> Annotated[Dict, Field(description='A monitor plus recent events, or a filtered list of monitors.')]:
        monitors = self.bronto_client.list_monitors()
        if monitor_id:
            match = next((m for m in monitors if str(m.get('id') or m.get('monitor_id')) == monitor_id), None)
            if match is None:
                return {'monitor': None, 'events': []}
            return {'monitor': match, 'events': self.bronto_client.get_monitor_events(monitor_id)}
        if status:
            s = status.lower()
            monitors = [m for m in monitors if str(m.get('status') or m.get('state') or '').lower() == s]
        if query:
            q = query.lower()
            monitors = [m for m in monitors if q in json.dumps(m, default=str).lower()]
        return {'monitors': monitors, 'total': len(monitors)}

    def monitor_log_ids(self):
        """Map each monitor id -> the log ids its backing query reads (explicit `from` only).

        Returns (monitor_ids_by_log_id, monitor_name_by_id). from_expr selectors are not
        resolved here (they need a catalogue lookup); such monitors contribute no log ids.
        """
        defs = {str(d.get('id')): d for d in self.bronto_client.list_metric_definitions() if d.get('id')}
        ids_by_log = {}
        name_by_id = {}
        for mon in self.bronto_client.list_monitors():
            mid = str(mon.get('id') or mon.get('monitor_id') or '')
            metric_id = str(mon.get('metric_id') or '')
            if not mid or not metric_id:
                continue
            name_by_id[mid] = mon.get('name')
            for q in (defs.get(metric_id, {}).get('queries') or []):
                if not isinstance(q, dict):
                    continue
                frm = q.get('from')
                log_ids = frm if isinstance(frm, list) else ([frm] if frm else [])
                for lid in log_ids:
                    ids_by_log.setdefault(str(lid), []).append(mid)
        return ids_by_log, name_by_id

    def validate_monitors(self) -> Annotated[
        Dict, Field(description='Per-monitor health: ok (data present), warn (valid, no data), error (cannot fire).')
    ]:
        defs = {str(d.get('id')): d for d in self.bronto_client.list_metric_definitions() if d.get('id')}
        results = []
        summary = {'ok': 0, 'warn': 0, 'error': 0}
        for mon in self.bronto_client.list_monitors():
            mid = str(mon.get('id') or mon.get('monitor_id') or '')
            name = mon.get('name')
            metric_id = str(mon.get('metric_id') or '')
            query = next((q for q in (defs.get(metric_id, {}).get('queries') or [])
                          if isinstance(q, dict)), None)
            if not query or not (query.get('from') or query.get('from_expr')):
                status, reason = 'error', 'monitor has no resolvable backing query'
            else:
                frm = query.get('from')
                log_ids = frm if isinstance(frm, list) else ([frm] if frm else [])
                body = {'select': ['count(*)'], 'time_range': 'Last 24 hours'}
                if log_ids:
                    body['from'] = log_ids
                elif query.get('from_expr'):
                    body['from_expr'] = query['from_expr']
                if str(query.get('where') or '').strip():
                    body['where'] = query['where']
                try:
                    resp = self.bronto_client.run_search(body)
                    totals = resp.get('totals') if isinstance(resp, dict) else None
                    count = totals.get('count') if isinstance(totals, dict) else None
                    if count and int(count) > 0:
                        status, reason = 'ok', 'data present'
                    else:
                        status, reason = 'warn', 'query valid but no data in the last 24 hours'
                except (BrontoResponseException, FailedBrontoRequestException,
                        BrontoResponseDecodingException) as exc:
                    status, reason = 'error', f'query failed: {exc}'
            summary[status] += 1
            results.append({'monitor_id': mid, 'name': name, 'status': status, 'reason': reason})
        return {'summary': summary, 'results': results}
