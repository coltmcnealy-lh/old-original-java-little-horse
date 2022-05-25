import json
import logging
import subprocess
import time
from typing import List, Optional, TypeVar

import requests

from lh_lib.schema.wf_spec_schema import ExternalEventDefSchema, LHDeployStatus, TaskDefSchema
from lh_sdk.compile import SpecsResult
from lh_sdk.utils import LHBaseModel
from lh_lib.config import DEFAULT_API_URL
from lh_lib.schema import RESOURCE_TYPES_INV, wf_run_schema
from lh_lib.schema.lh_rpc_response_schema import LHRPCResponseSchema, ResponseStatusEnum

from lh_lib.schema import *

T = TypeVar("T")


class IndexEntrySchema(LHBaseModel):
    object_id: str
    first_offset: Optional[int] = None
    most_recent_offset: Optional[int] = None


class RangeQueryResultSchema(LHBaseModel):
    token: Optional[str] = None
    object_ids: List[str]


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

        idx_response: LHRPCResponseSchema[RangeQueryResultSchema] =\
            self.search(
            resource_type,
            "name",
            resource_id,
        )

        if idx_response.result is not None and len(idx_response.result.object_ids):
            new_id = idx_response.result.object_ids[0]
            return self.get_resource_by_id(resource_type, new_id)

        return first_try  # an empty one ):

    def delete_resource_by_id(
        self,
        resource_type: type[T],
        resource_id: str,
    ) -> LHRPCResponseSchema[T]:
        resource_type_name = RESOURCE_TYPES_INV[resource_type]
        url = f'{self.url}/{resource_type_name}/{resource_id}'

        response = requests.delete(url)
        response.raise_for_status()

        out = LHRPCResponseSchema(**response.json())
        if out.result is not None:
            t_constructor = globals()[resource_type.__name__]
            out.result = t_constructor(**out.result)

        return out

    def search(
        self,
        resource_type: type,
        key: str,
        val: str,
        limit: Optional[int] = None,
        token: Optional[str] = None,
    ) -> LHRPCResponseSchema[RangeQueryResultSchema]:
        resource_type_name = RESOURCE_TYPES_INV[resource_type]

        url = f"{self.url}/search/{resource_type_name}/{key}/{val}"
        params = {}
        if token is not None:
            params['token'] = token
        if limit is not None:
            params['limit'] = str(limit)

        response = requests.get(url, params=params)
        response.raise_for_status()

        intermediate = LHRPCResponseSchema(**response.json())
        if intermediate.result is not None:
            intermediate.result = RangeQueryResultSchema(
                **intermediate.result
            )
        return intermediate

    def list(
        self,
        resource_type: type,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = None,
        token: Optional[str] = None,
    ) -> LHRPCResponseSchema[RangeQueryResultSchema]:
        resource_type_name = RESOURCE_TYPES_INV[resource_type]

        url = f"{self.url}/search/{resource_type_name}/{key}/{val}"
        params = {}
        if token is not None:
            params['token'] = token
        if limit is not None:
            params['limit'] = str(limit)

        response = requests.get(url, params=params)
        response.raise_for_status()

        intermediate = LHRPCResponseSchema(**response.json())
        if intermediate.result is not None:
            intermediate.result = RangeQueryResultSchema(
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

    def deploy_specs(
        self, specs: SpecsResult, skip_build=False, docker_push_step=None
    ):
        if not skip_build:
            for task_def_name in specs.dockerfile:
                self.build_docker(
                    specs.dockerfile[task_def_name],
                    task_def_name,
                    docker_push_step,
                )

        for task_def in specs.task_def:
            self.add_task_def(task_def)

        for external_event in specs.external_event_def:
            self.add_external_event_def(external_event)
    
        time.sleep(0.5)

        for wf_spec in specs.wf_spec:
            self.add_wf_spec(wf_spec)

    def build_docker(
        self,
        dockerfile: str,
        task_def_name: str,
        docker_push_step=None,
    ):
        tag = f'lh-task-{task_def_name}:latest'
        cmd = ['docker', 'build', '-f', '-', '-t', tag, '.']
        subprocess.run(cmd, input=dockerfile, text=True)

        if docker_push_step is None:
            return

        cmd = docker_push_step.split()
        for i in range(len(cmd)):
            if cmd[i] == "<<image>>":
                cmd[i] = tag

        subprocess.run(cmd, text=True)

    def add_external_event_def(self, ee: ExternalEventDefSchema):
        url = f'{self.url}/ExternalEventDef'
        requests.post(
            url, json=json.loads(ee.json(by_alias=True))
        ).raise_for_status()

    def add_wf_spec(self, wf: WFSpecSchema):
        url = f'{self.url}/WFSpec'
        response = requests.post(
            url, json=json.loads(wf.json(by_alias=True))
        )
        try:
            response.raise_for_status()
        except Exception as exn:
            logging.error(
                f"Got an exception: {exn}, {response.content.decode()}"
            )
            raise exn
        print("Got id: ", response.json()['objectId'])
        return response.json()['objectId']

    def add_task_def(self, td: TaskDefSchema):
        url = f'{self.url}/TaskDef'
        response = requests.post(
            url, json=json.loads(td.json(by_alias=True))
        )
        try:
            response.raise_for_status()
        except Exception as exn:
            print(response.content.decode())
            raise exn

    def send_event_by_name_or_id(
        self,
        name_or_id: str,
        wf_run_id: str,
        payload: str,
    ):
        query = self.get_resource_by_name_or_id(
            ExternalEventDefSchema,
            name_or_id,
        )

        assert query.result is not None, "Provided ExternalEventDef not found!"

        send_response = requests.post(
            f'{self.url}/externalEvent/{query.result.id}/{wf_run_id}',
            data=payload,
        )
        send_response.raise_for_status()
        return query.result.id
