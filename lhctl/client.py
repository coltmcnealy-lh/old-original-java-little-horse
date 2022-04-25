from typing import List, Optional, TypeVar
import requests
from lh_sdk.utils import LHBaseModel

from lhctl.config import DEFAULT_API_URL
from lhctl.schema import RESOURCE_TYPES_INV, wf_run_schema
from lhctl.schema.lh_rpc_response_schema import LHRPCResponseSchema

from lhctl.schema import *

T = TypeVar("T")


class AliasEntrySchema(LHBaseModel):
    object_id: str
    first_offset: Optional[int] = None
    most_recent_offset: Optional[int] = None


class AliasEntryCollectionSchema(LHBaseModel):
    entries: List[AliasEntrySchema]


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
        """Returns an LHRPCResponse[T] with either the T from the provided resource_id
        or the most recent T with the provided name.
        """
        first_try = self.get_resource_by_id(resource_type, resource_id)
        if first_try.result is not None:
            return first_try

        alias_response: LHRPCResponseSchema[AliasEntryCollectionSchema] =\
            self.search_for_alias(
            resource_type,
            "name",
            resource_id,
        )

        if alias_response.result is not None and len(alias_response.result.entries):
            new_id = alias_response.result.entries[0].object_id
            return self.get_resource_by_id(resource_type, new_id)

        return first_try  # an empty one ):

    def search_for_alias(
        self,
        resource_type: type,
        key: str,
        val: str,
    ) -> LHRPCResponseSchema[AliasEntryCollectionSchema]:
        resource_type_name = RESOURCE_TYPES_INV[resource_type]

        url = f"{self.url}/{resource_type_name}Alias/{key}/{val}"
        response = requests.get(url)
        response.raise_for_status()

        intermediate = LHRPCResponseSchema(**response.json())
        if intermediate.result is not None:
            intermediate.result = AliasEntryCollectionSchema(
                **intermediate.result
            )
        return intermediate

    def run_wf(
        self,
        wf_spec_id_or_name: str,
        vars: Optional[dict] = None,
        wf_run_id: Optional[str] = None,
    ) -> LHRPCResponseSchema[WFRunSchema]:
        wf_run_request = {
            "wfSpecId": wf_spec_id_or_name,
            "variables": vars,
            "wfRunId": wf_run_id
        }

        run_wf_response = requests.post(
            f"{self.url}/WFRun",
            json=wf_run_request,
        )

        intermediate = LHRPCResponseSchema(**run_wf_response.json())
        if intermediate.result is not None:
            intermediate.result = WFRunSchema(**intermediate.result)

        return intermediate