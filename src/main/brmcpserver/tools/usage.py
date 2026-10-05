"""Usage tool: per-dataset ingestion and search bytes against org limits."""

from typing import Dict, Optional

from bronto.client import BrontoClient
from pydantic import Field
from typing_extensions import Annotated


class UsageTools:
    """Tools to report Bronto ingestion and search usage"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    def get_usage(
            self,
            time_range: Annotated[Optional[str], Field(default='This month', description='Relative window; limits are per calendar month.')] = 'This month',
    ) -> Annotated[Dict, Field(description='Per-dataset ingestion/search bytes and org totals vs monthly limits.')]:
        ingest = {r['log_id']: r for r in self.bronto_client.get_usage_by_log('ingestion', time_range)}
        search = {r['log_id']: r for r in self.bronto_client.get_usage_by_log('search', time_range)}
        datasets = {}
        for lid in set(ingest) | set(search):
            datasets[lid] = {
                'log_id': lid,
                'ingest_bytes': ingest.get(lid, {}).get('bytes_total', 0),
                'search_bytes': search.get(lid, {}).get('bytes_total', 0),
            }
        # Org SYSTEM limits: ingestion is bytes; search is a ratio of the ingestion limit.
        ingest_limit = None
        search_ratio = None
        for lim in self.bronto_client.get_usage_limits():
            if lim.get('target') != 'ORGANISATION' or lim.get('type') != 'SYSTEM':
                continue
            if lim.get('category') == 'INGESTION_LIMITS' and lim.get('unit') == 'BYTES':
                ingest_limit = int(lim.get('value') or 0)
            elif lim.get('category') == 'SEARCH_LIMITS' and lim.get('unit') == 'RATIO':
                search_ratio = lim.get('value')
        summary = {
            'datasets_total': len(datasets),
            'ingest_bytes': sum(d['ingest_bytes'] for d in datasets.values()),
            'search_bytes': sum(d['search_bytes'] for d in datasets.values()),
        }
        if ingest_limit is not None:
            summary['ingest_limit'] = ingest_limit
            if search_ratio is not None:
                summary['search_limit'] = int(search_ratio * ingest_limit)
        return {'summary': summary, 'datasets': sorted(datasets.values(),
                key=lambda d: d['ingest_bytes'], reverse=True)}
