"""Dataset and key discovery tools."""

from typing import Dict, List

from bronto.client import BrontoClient
from bronto.models import Dataset
from pydantic import Field
from typing_extensions import Annotated


class DatasetTools:
    """Tools to discover Bronto datasets and the keys their data contains"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    def get_datasets(self) -> Annotated[
        List[Dataset],
        Field(description='''List of datasets. Each dataset object contains
                - the name of the dataset
                - the collection it belongs to
                - its log ID, which is a UUID, i.e. a 36 character long string
                - a list of tags associated to the dataset. Each tag is a key-value pair. Both keys and values are 
                represented as strings. Tags such as the `description` tag are particularly useful to understand the type 
                of data that the dataset contains. Other common tags are `service`, `teams` and `environment`''')
    ]:
        datasets_data = self.bronto_client.get_datasets()
        result = []
        for dataset in datasets_data:
            result.append(Dataset(name=dataset["log"], collection=dataset["logset"], log_id=dataset["log_id"],
                                  tags=dataset["tags"]))
        return result

    def get_datasets_by_name(
            self,
            dataset_name: Annotated[str, Field(description="The dataset name", min_length=1)],
            collection_name: Annotated[str, Field(description="The collection that the dataset is part of", min_length=1)]
    ) -> Annotated[
        List[Dataset],
        Field(description='List of datasets whose name and collection match the ones provided with the `dataset_name` '
                          'and `collection_name` parameters. Details contains for instance the dataset log ID as well '
                          'as all the tags associated to this dataset.')
    ]:
        datasets = self.bronto_client.get_datasets()
        result = []
        collection_names = [dataset['logset'] for dataset in datasets]
        if len(collection_names) == 0:
            return []
        for dataset in datasets:
            if dataset['log'] != dataset_name or dataset['logset'] != collection_name:
                continue
            result.append(Dataset(name=dataset["log"], collection=dataset["logset"], log_id=dataset["log_id"],
                                  tags=dataset["tags"]))
        if len(result) == 0:
            return []
        return result

    def get_dataset_keys(
            self,
            log_id: Annotated[str, Field(description='The dataset ID, also named log ID', min_length=36, max_length=36)]
    ) -> Annotated[
        List[str],
        Field(description='list key names for keys present in the provided dataset referenced with the `log_id` parameter')
    ]:
        keys = [dataset.name for dataset in self.bronto_client.get_keys(log_id)]
        return keys

    def get_all_datasets_keys(self) -> Annotated[
        Dict[str, List[str]],
        Field(description='Map from dataset IDs to the list of key names, for keys present in each dataset')
    ]:
        keys = self.bronto_client.get_all_datasets_top_keys()
        return keys

    def get_key_values(
            self,
            key: Annotated[str, Field(description='The name of a key')],
            log_id: Annotated[str, Field(description='A string representing a dataset ID')]
    ) -> Annotated[List[str], Field(description='The list of values of the provided key, present in the provided '
                                                'dataset.')]:
        datasets_top_keys_and_values = self.bronto_client.get_all_datasets_top_keys_and_values()
        keys_and_values = datasets_top_keys_and_values.get(log_id, {})
        return keys_and_values.get(key, [])
