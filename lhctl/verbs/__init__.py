from lhctl.verbs.handle_get import GETHandler
from lhctl.verbs.handle_run import RUNHandler


HANDLERS = {
    "get": GETHandler(),
    "run": RUNHandler(),
}