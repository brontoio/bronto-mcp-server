"""Error summary tool: org-wide parsing/ingestion errors and warnings."""

from typing import Dict, Optional

from bronto.client import BrontoClient
from pydantic import Field
from typing_extensions import Annotated


class ErrorTools:
    """Tools to summarise Bronto parsing/ingestion errors and warnings"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    def get_error_summary(
            self,
            time_range: Annotated[Optional[str], Field(default='Last 24 hours', description='Relative window, e.g. "Last 24 hours".')] = 'Last 24 hours',
    ) -> Annotated[Dict, Field(description='Org-wide error/warning totals and a per-dataset breakdown.')]:
        raw = self.bronto_client.get_error_analytics(time_range)
        index = {str(d.get('log_id')): d for d in self.bronto_client.get_datasets() if d.get('log_id')}
        return self._shape_error_summary(raw, index)

    @staticmethod
    def _shape_error_summary(raw, dataset_index) -> Dict:
        """Shape an /analytics/errors payload into {errors, warnings, by_dataset?}.

        groups_series carries an "Error" and a "Warn" group, each with a total count and a
        nested groups_series whose name is the dataset log_id. by_dataset lists only the
        datasets with errors/warnings, enriched with name/collection from dataset_index.
        """
        def _count(node):
            return node.get('count') if node.get('count') is not None else node.get('value') or 0

        totals = {'errors': 0, 'warnings': 0}
        datasets = {}
        groups = raw.get('groups_series') if isinstance(raw, dict) else None
        for group in groups or []:
            if not isinstance(group, dict):
                continue
            # Group names are "Error" and "Warn"; match by prefix.
            name = str(group.get('name') or '').strip().lower()
            key = 'errors' if name.startswith('error') else 'warnings' if name.startswith('warn') else None
            if key is None:
                continue
            totals[key] = _count(group)
            for sub in group.get('groups_series') or []:
                if not isinstance(sub, dict) or sub.get('name') is None:
                    continue
                log_id = str(sub['name'])
                meta = dataset_index.get(log_id, {})
                entry = datasets.setdefault(log_id, {
                    'log_id': log_id, 'name': meta.get('log'), 'collection': meta.get('logset'),
                    'errors': 0, 'warnings': 0})
                entry[key] = _count(sub)
        result = dict(totals)
        if datasets:
            result['by_dataset'] = sorted(datasets.values(),
                                          key=lambda d: (-(d['errors'] + d['warnings']), d['log_id']))
        return result
