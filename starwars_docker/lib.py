import json


def form_task(name, bash_command, required_vars=None):
    out = {
        "name": name,
        "deployMetadata": json.dumps({
            "dockerImage": "little-horse-api:latest",
            "metadata": json.dumps({
                "bashCommand": ['python3'] + bash_command
            }),
            "secondaryValidatorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashValidator",
            "taskExecutorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashExecutor",
        }),
        "taskDeployerClassName": "little.horse.lib.deployers.examples.docker.DockerTaskDeployer"
    }

    if required_vars is not None:
        out['requiredVars'] = {}
        for var in required_vars.keys():
            out['requiredVars'][var] = {
                "type": required_vars[var]
            }

    return out