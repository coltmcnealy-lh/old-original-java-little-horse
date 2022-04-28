# Simple Heterogeneous Workflow

This subdirectory is a simple POC of running a workflow with tasks implemented in different languages. There is a standard `lhctl` python SDK workflow file, `heterogeneous.py`. One of the tasks passes in a String rather than a python function. The String refers to the name of a `TaskDef`, implying that the `TaskDef` has already been deployed before the `WFSpec` has been deployed. The `WFRun` will simply execute the task with the specified name at that point.

## Building the Task

The task code is super-simple and lives in `MyTask.java`. To build, simply run the `./build-task.sh` script. It will compile the Java code and build a docker image that gets deployed by the `task-def-spec.json` file.

## Deploying the Workflow

To deploy this workflow, there are a few steps:

1. Build the task image by running: `./build-task.json`
2. Deploy the `TaskDef` by running: `lhctl deploy --file ./task-def-spec.json`
3. Deploy the `WFSpec` by running: `lhctl deploy --wf-func heterogeneous.heterogeneous_wf`

## Running the Workflow

`lhctl run heterogeneous_wf`
