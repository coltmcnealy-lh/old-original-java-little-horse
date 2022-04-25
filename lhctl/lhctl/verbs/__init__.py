from lhctl.verbs.handle_get import GETHandler
from lhctl.verbs.handle_run import RUNHandler
from lhctl.verbs.handle_compile import COMPILEHandler


HANDLERS = {
    "get": GETHandler(),
    "run": RUNHandler(),
    "compile": COMPILEHandler(),
}