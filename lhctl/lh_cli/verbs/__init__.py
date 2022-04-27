from lh_cli.verbs.handle_delete import DELETEHandler
from lh_cli.verbs.handle_get import GETHandler
from lh_cli.verbs.handle_run import RUNHandler
from lh_cli.verbs.handle_compile import COMPILEHandler
from lh_cli.verbs.handle_deploy import DEPLOYHandler
from lh_cli.verbs.handle_send_event import SENDEVENTHandler


HANDLERS = {
    "get": GETHandler(),
    "run": RUNHandler(),
    "compile": COMPILEHandler(),
    "deploy": DEPLOYHandler(),
    "delete": DELETEHandler(),
    "send-event": SENDEVENTHandler(),
}