import os


DEFAULT_WF_DEPLOYER_KEY = "LHORSE_DEFAULT_WF_DEPLOYER"
DEFAULT_TASK_DEPLOYER_KEY = "LHORSE_DEFAULT_TASK_DEPLOYER"

def get_wf_deployer_class():
    return os.getenv(
        DEFAULT_WF_DEPLOYER_KEY,
        "little.horse.lib.deployers.examples.docker.DockerWorkflowDeployer"
    )


def get_wf_deploy_metadata():
    pass
