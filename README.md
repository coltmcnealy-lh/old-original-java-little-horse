# LittleHorse Runtime

- [LittleHorse Runtime](#littlehorse-runtime)
  - [Development](#development)
    - [Dependencies](#dependencies)
    - [Building LittleHorse Java Code](#building-littlehorse-java-code)
    - [Installing `lhctl`](#installing-lhctl)
  - [Understanding the Code](#understanding-the-code)
    - [Repository Structure](#repository-structure)
    - [Where the Logic Is](#where-the-logic-is)
    - [Entrypoint Classes](#entrypoint-classes)
  - [Running Examples](#running-examples)
    - [Build the Main Docker Image](#build-the-main-docker-image)
    - [Install in Docker Compose](#install-in-docker-compose)
    - [Run a Workflow](#run-a-workflow)
    - [Cleanup](#cleanup)

This repository contains code for the LittleHorse Runtime.

## Development
This section is a quickstart on running and developing LittleHorse locally.

### Dependencies
The following software is needed to develop LittleHorse:

* `openjdk`, tested with version `17.0.2 2022-01-18`.
* `gradle` version `7.3` or later.
* `docker` CLI and engine, tested with version `20.10.14`.
* `docker-compose`, tested with version `1.29.2`.
* `anaconda3`
* Optional: `kubectl` version 16 or later.
* Optional: `kind` version `v0.11.1` or later.
* Optional, Not Recommended: `pip`

### Building LittleHorse Java Code
Running the script `./build.sh` should build LittleHorse, creating an all-in-one docker image tagged `little-horse-api:latest`.

To simply build the Java binary (but not the docker image), run `gradle build` in the root of the repository. This will allow you to run an arbitrary class using the command `java -cp "./app/bin/main:./app/build/libs/app-all.jar" little.horse.SomeClass` from the root of the repository.

### Installing `lhctl`
LittleHorse comes with the `lhctl` python package, which provides an SDK/Interpreter for developing workflows and a CLI for managing and running workflows. In order to install `lhctl`, you must install python (I recommend doing so through [anaconda](https://docs.anaconda.com/anaconda/install/index.html)).

In order to install `lhctl`, you must do three things:
1. Create and activate a `conda` environment with the necessary dependencies.
2. Add `/path/to/little-horse/lhctl` to your `PYTHONPATH`.
3. Alias `lhctl` to `python -m lh_cli`.

To create the conda environment, I suggest just using the `environment.dev.yml` file rather than wrestling with `pip` and all of your system dependencies:

```
conda env update -f environment.dev.yml
conda activate little-horse
```

To add `lhctl` to your `PYTHONPATH`, do the following (substitute out `/path/to`):
```
export PYTHONPATH=$PYTHONPATH:/path/to/little-horse/lhctl/
```

To "install" `lhctl`, do the following:
```
alias lhctl='python -m lh_cli'
```

*Optional: I recommend adding the following to your `.bashrc` or `.bash_profile`:*
```
export PYTHONPATH=$PYTHONPATH:/path/to/little-horse/lhctl # Remember to sub /path/to
conda activate little-horse
alias lhctl='python -m lh_cli'
```

## Understanding the Code

For a primer on the LittleHorse Architecture, see this [Google Document](https://docs.google.com/document/d/1_6jWS46350NIKkczICFDgVFA88zZEBXBC_Isq-Bv9iQ/edit?usp=sharing). You may need to request access from `colt@littlehorse.io`.

Next, read the `docs/PROGRAMMING_MODEL.md` to understand the behavior of the system. Lastly, it is recommmended that you familiarize yourself with Kafka *and specifically* the Kafka Streams library.

### Repository Structure

The repository has the following components:
* `app/src/main/java/little/horse/api/`: The core API server is implemented in this folder.
* `app/src/main/java/little/horse/common/`
    * `objects/`: All objects in the LittleHorse Programming model (see `PROGRAMMING_MODEL.md`) are implemented in this subdirectory.
        * `metadata`: Code in this directory is used for administrative metadata, such as Workflow Specifications (`WFSpec`), Task Definitions (`TaskDef`), and others.
        * `rundata`: Code in this directory is involved in the logic of a Workflow Run (`WFRun`). The majority of the interesting stuff is in the `ThreadRun.java`.
    * `events/`: contains schemas for events involved in a `WFRun`, such as `TaskScheduledEvent`, `TaskCompletedEvent`, and `ExternalEvent`.
    * `util` and `exceptions` contain utility code.
* `app/src/main/java/little/horse/deployers/`: This directory contains a) Java interfaces to implement Task/Workflow Deployers and Workers, and b) example implementations of those interfaces.
    * `examples`: Example implementations of the `TaskDeployer.java` and `WorkflowDeployer.java` interfaces.
      * `docker`, `kubernetes`: Example implementations to deploy to docker and kubernetes, respectively.
* `app/src/main/java/little/horse/sdk`: This contains a semi-abandoned prototype for a workflow-authoring SDK. It has been neglected in favor of the `lhctl` directory.
* `lhctl`: The Python SDK and Test Harness.
  * `lh_sdk`: Contains a python library for authoring workflows as code.
  * `executor`: Contains a python library and executable used for turning a standard python function into a `TaskDef` worker.
  * `lh_test_harness`: Contains a set of integration tests that verify that workflows run as they should. More functionality will soon be added.
  * `lh_cli`: Implements the actual `lhctl` command, which is patterned after `kubectl`.

### Where the Logic Is
* Core API:
    * `BaseSchema.java` is a basic serializable class (through JSON dumping/loading) used to store all imporant data.
    * `CoreMetadata.java` is a base for special metadata, such as all objects in the `PROGRAMMING_MODEL.md`, including `WFSpec`, `TaskDef`, `ExternalEventDef`, and `WFRun`. 
    * `MetadataTopologyBuilder.java` is a generic class which initializes a Kafka Streams Topology for a specifc CoreMetadata type. It handles the CRUD operations for that type, and uses the associated CoreMetadata's implementation in order to produce side effects (eg deploying a Workflow).
    * `CoreMetadataAPI.java` is the generic class that implements CRUD for Core Metadata in the REST API.
        * *Note: it is all put together in the `LittleHorseAPI.java` class.*
    * `APIStreamsContext.java` is the high-tech class used to turn Kafka Streams into a database, allowing for querying data partitioned across several RocksDB instances.
* Workflow Scheduling Logic
    * `SchedulerTopology.java` initializes the Kafka Streams Topology on the Workflow Worker process. This topology is the actual scheduler that sends task schedule requests to appropriate task queues, and updates the state of the WFRun in a Kafka Topic that is listened to by the CoreMetadata Topology on the Core LittleHorse API.
    * `SchedulerProcessor.java` shepherds the actual scheduling. See how it gets inserted in `SchedulerTopology.java`.
    * `WFRun.java` and `ThreadRun.java` have the real potatoes of the actual state transition logic for what happens when an event (i.e. Task Completed) is processed.
* Deployer Interfaces
    * `WorkflowDeployer.java` is the interface for deploying a workflow.
        * `DockerWorkflowDeployer.java` and `K8sWorkflowDeployer.java` are example implementations. Note that they both use the `Scheduler.java` class, see the K8s entrypoint and the Docker command.
    * `TaskDeployer.java` is the interface for deploying tasks.
        * `DockerTaskDeployer.java` and `K8sWorkflowDeployer.java` are example implementations.

### Entrypoint Classes
* `LittleHorseAPI.java` is run for the core API.
* `TaskWorker.java` is the entrypoint class used by the `DockerTaskDeployer` and `K8sWorkflowDeployer` classes. It takes in a name of an implementation of the `JavaTask.java` interface, and executes the function for each `TaskScheduleRequest`.
  * *This is analagous to `python -m executor` for python-based tasks.*
* `Scheduler.java` is an example of a process that does the Workflow Scheduling.

## Running Examples

After installing all dependencies (see above), you can run a simple example in docker by the following.

### Build the Main Docker Image

`./build.sh`

### Install in Docker Compose

`./docker/setup.sh`

If you run `docker ps`, you should see the following:
```
-> docker ps
CONTAINER ID   IMAGE                             COMMAND                  CREATED         STATUS        PORTS     NAMES
d63da278489a   little-horse-api:latest           "java -cp /littleHor…"   2 seconds ago   Up 1 second             little-horse-api
2f97f257f8e9   confluentinc/cp-kafka:6.2.0       "/etc/confluent/dock…"   2 seconds ago   Up 1 second             broker
df1608e502ab   confluentinc/cp-zookeeper:6.2.0   "/etc/confluent/dock…"   2 seconds ago   Up 1 second             zookeeper

```
There is one container for the core API server, a kafka broker, and zookeeper (used for kafka).

### Run a Workflow

*Important:* This section assumes that your current working directory is `examples/`.

Let's run the `basic_wf` workflow. The `basic_wf` workflow is quite simple. It consists of three Node's:

1. The `ask_for_name` Node executes the `whats-your-name` task.
2. The `wait_for_name` Node waits for the `my-name` External Event to come in, and updates the `person_name` variable to be the value provided in the External Event.
3. The `greet` Node executes the `hell-there` Task, providing `person_name` as an input variable.

Look at the code in `examples/workflows/basic_wf.py`. It should be pretty self-explanatory given all the comments ;)

The `my_workflow()` function in `basic_wf.py` defines the `WFSpec`. The other functions become `TaskDef`'s (both `WFSpec` and `TaskDef` are JSON in their true form). To see the raw JSON Spec output, navigate to the `examples` directory and then run the following:

```
lhctl compile workflows.basic_wf.my_workflow
```
This command is purely a dry-run command which shows you the specs that will be deployed in our next step.

*NOTE: try piping the above command to `| jq .` to format it, or paste it to an online json pretty printer.*

You can see that there are `TaskDef`, `WFSpec`, `ExternalEventDef`, and `Dockerfile` entries. The `Dockerfile`'s build images that run the `TaskDef`'s (it's a mild but pretty cool hack in `lhctl`).

Now let's build and deploy the workflow:
```
lhctl deploy --wf-func workflows.basic_wf.my_workflow
```

This will build the docker task workers, deploy the `TaskDef`'s and `ExternalEventDef`'s for the workflow, and finally deploy the `WFSpec`.

You should see something like the following via `docker ps`:
```
->docker ps
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS          PORTS     NAMES
126232f0153b   little-horse-api:latest           "java -cp /littleHor…"   28 seconds ago   Up 28 seconds             my-workflow-9f22faaa
7ffff8f705fb   lh-task-greet:latest              "java -cp /littleHor…"   29 seconds ago   Up 29 seconds             lh-task-greet
66d685caa40e   lh-task-ask_for_name:latest       "java -cp /littleHor…"   30 seconds ago   Up 29 seconds             lh-task-ask_for_name
e359c918ccd1   little-horse-api:latest           "java -cp /littleHor…"   2 hours ago      Up 2 hours                little-horse-api
9aef4923f2d0   confluentinc/cp-kafka:6.2.0       "/etc/confluent/dock…"   2 hours ago      Up 2 hours                broker
ee0c8816dfea   confluentinc/cp-zookeeper:6.2.0   "/etc/confluent/dock…"   2 hours ago      Up 2 hours                zookeeper

```

The `my-workflow-9f22faaa` is the Workflow Scheduler; the `lh-task-ask_for_name` is the Task Worker for the `ask_for_name` task function; and the `lh-task-greet` is the Task Worker for the `greet` task function.


To run the workflow, do the following:
```
-> lhctl run my_workflow
{"message": null, "status": "OK", "object_id": "e58811dd-38bb-4229-a83d-a9607239423c", "result": null}

```
The long guid thing is the ID of the Workflow Run you've just created. You can check its status via:

```
-> lhctl get WFRun e58811dd-38bb-4229-a83d-a9607239423c
WFRun Status:RUNNING
 Threads:
--->>
	 Id: 0
	 Status: RUNNING
	 Tasks:
		 0-ask_for_name: Hey what's your name?
	 Waiting on node 1-wait-event-my-name
	 Variables:
		 my_name_var: null
```
The WFRun is stuck waiting on the node `wait-event-my-name`, which means it's waiting for an External Event. Perfect! That's what's supposed to happen. Let's send an External Event to get things moving again:

```
->lhctl send-event e58811dd-38bb-4229-a83d-a9607239423c my-name Obi-Wan
Successfully sent ExternalEvent with ExternalEventDefId my-name.
```

And let's look at the workflow status to make sure it's completed:

```
-> lhctl get WFRun e58811dd-38bb-4229-a83d-a9607239423c
 WFRun Status:COMPLETED
 Threads:
--->>
	 Id: 0
	 Status: COMPLETED
	 Tasks:
		 0-ask_for_name: Hey what's your name?
		 1-wait-event-my-name: Obi-Wan
		 2-greet: Hello there, name!
	 Variables:
		 my_name_var: "Obi-Wan"

```
You can see that the External Event was saved to the `my_name` variable, and the Workflow Run has completed. Great!!

You can search for workflows by their variables. For example:
```
->lhctl search WFRun my_name_var foobar
[]

->lhctl search WFRun my_name_var Obi-Wan
["e58811dd-38bb-4229-a83d-a9607239423c"]

```

### Cleanup

To clean up the LittleHorse Core API and Kafka containers and delete everything in the workflow you just ran, you can:
```
-> ./docker/cleanup.sh
```
