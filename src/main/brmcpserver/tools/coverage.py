"""Coverage tool: which datasets are monitored, searched and still ingesting."""

import time
from typing import Dict, Optional

from bronto.client import BrontoClient
from pydantic import Field
from typing_extensions import Annotated

from tools.monitors import MonitorTools

# A dataset whose last heartbeat is within this window counts as still ingesting.
ACTIVE_WINDOW_DAYS = 14


class CoverageTools:
    """Tools to report monitoring, search and ingestion coverage per dataset"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client
        self.monitors = MonitorTools(bronto_client)

    def get_coverage(
            self,
            time_range: Annotated[Optional[str], Field(default='This month', description='Window for the searched/ingesting axes.')] = 'This month',
    ) -> Annotated[Dict, Field(description='Per-dataset coverage: monitors (ids; [] = blind spot), searched, ingesting.')]:
        datasets = self.bronto_client.get_datasets()
        ids_by_log, _ = self.monitors.monitor_log_ids()
        ingest = {r['log_id']: r for r in self.bronto_client.get_usage_by_log('ingestion', time_range)}
        search = {r['log_id']: r for r in self.bronto_client.get_usage_by_log('search', time_range)}
        # The usage API omits non-billable system collections (.usage, .audit-trail), so
        # ingestion activity also falls back to the dataset's last heartbeat.
        heartbeat_cutoff = int(time.time() * 1000) - ACTIVE_WINDOW_DAYS * 24 * 3600 * 1000
        out = []
        for d in datasets:
            lid = str(d.get('log_id') or '')
            if not lid:
                continue
            meta = d.get('metadata') if isinstance(d.get('metadata'), dict) else {}
            heartbeat = meta.get('last_heartbeat_at')
            ingestion = {'active': ingest.get(lid, {}).get('count', 0) > 0
                         or (heartbeat is not None and int(heartbeat) >= heartbeat_cutoff),
                         'bytes': ingest.get(lid, {}).get('bytes_total', 0)}
            if heartbeat is not None:
                ingestion['last_heartbeat_at'] = heartbeat
            out.append({
                'log_id': lid,
                'name': d.get('log'),
                'collection': d.get('logset'),
                'monitors': ids_by_log.get(lid, []),
                'search': {'active': search.get(lid, {}).get('count', 0) > 0,
                           'bytes': search.get(lid, {}).get('bytes_total', 0)},
                'ingestion': ingestion,
            })
        summary = {
            'datasets_total': len(out),
            'monitored': sum(1 for d in out if d['monitors']),
            'unmonitored': sum(1 for d in out if not d['monitors']),
        }
        return {'summary': summary, 'datasets': out}
