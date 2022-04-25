from lh_cli.verbs.handle_get import GETHandler
from lh_cli.verbs.handle_run import RUNHandler
from lh_cli.verbs.handle_compile import COMPILEHandler


HANDLERS = {
    "get": GETHandler(),
    "run": RUNHandler(),
    "compile": COMPILEHandler(),
}