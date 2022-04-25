from typing import Optional, TypeVar
import requests

from lhctl.config import DEFAULT_API_URL
from lhctl.schema import RESOURCE_TYPES_INV
from lhctl.schema.lh_rpc_response_schema import LHRPCResponseSchema

from lhctl.schema import *

T = TypeVar("T")


class LHClient:
    def __init__(self, url=DEFAULT_API_URL):
        self._url = url

    @property
    def url(self) -> str:
        return self._url

    def get_resource_by_id(
        self,
        resource_type: type[T],
        resource_id: str,
    ) -> LHRPCResponseSchema[T]:

        resource_type_name = RESOURCE_TYPES_INV[resource_type]

        url = f'{self.url}/{resource_type_name}/{resource_id}'
        response = requests.get(url)
        response.raise_for_status()
        intermediate = LHRPCResponseSchema(**response.json())

        t_constructor = globals()[resource_type.__name__]
        t_result: Optional[T] = None
        if intermediate.result is not None:
            t_result = t_constructor(**intermediate.result)
        else:
            t_result = None

        intermediate.result = t_result
        return intermediate

    def get_resource_by_name_or_id(
        self,
        resource_type: type[T],
        resource_id: str,
    ) -> LHRPCResponseSchema[T]:
        raise NotImplemented()
