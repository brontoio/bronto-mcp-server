from typing import Dict, List, Annotated
from pydantic import BaseModel, Field


class Dataset(BaseModel):
    name: Annotated[str, Field(description='The dataset name. This often represents the name of the service that '
                                           'generates the log data that the dataset contains')]
    collection: Annotated[str, Field(description='The name of the collection that the dataset belongs to. The same '
                                                 'dataset name may be associated to different collections.')]
    log_id: Annotated[str, Field(description='The ID of the dataset. This ID is a UUID identifying a dataset uniquely')]
    tags: Annotated[Dict[str, str], Field(description='Tags are key-value pair associated to the dataset. For instance,'
                                                      '"environment"="production" may indicate that this dataset '
                                                      'contains data generated from a production environment.')]


class DatasetKey(BaseModel):
    name: Annotated[str, Field(description='The name of the key, e.g. `environment`, `$service`, etc')]
    values: Annotated[List[str], Field(default=[], description='Sample values of the key')]

    def add_values(self, values: List[str]) -> None:
        for value in values:
            if value in self.values:
                continue
            else:
                self.values.append(value)


class LogEvent(BaseModel):
    message: Annotated[str, Field(description='The message of the event. This represents a log message generated from a '
                                              'log statement in a piece of software')]
    attributes: Annotated[
        Dict[str, str],
        Field(default={}, description='List of attributes associated to the log event. An attribute is a key-value '
                                      'pair that provide context about the event. For instance, '
                                      '"$hostname"="i-1234567890" may indicate that the service generated the log event'
                                      ' on the host named "i-1234567890"')]

    def add_attribute(self, key: str, value: str) -> None:
        """ Associate the key=value key-value pair to the log event. If the key is already associated to the log event,
            then the previously associated value is overwritten with the new one.
        """
        self.attributes.update({key: value})


class Datapoint(BaseModel):
    timestamp: Annotated[int, Field(description='The unix epoch timestamp of the datapoint')]
    count: Annotated[int, Field(description='The number of events used to compute the metric value of the datapoint')]
    quantiles: Annotated[Dict[float, float], Field(description='Map containing quantiles values')]
    value: Annotated[float, Field(description='The value of the metric associated to the datapoint')]


class Timeseries(BaseModel):
    count: Annotated[int, Field(description='The number of events used to compute the total metric value')]
    timeseries: Annotated[List[Datapoint], Field(description='A list of datapoints per timestamp')]
