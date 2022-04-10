# LittleHorse Runtime

- [LittleHorse Runtime](#littlehorse-runtime)
  - [Development](#development)
    - [Dependencies](#dependencies)
    - [Building LittleHorse](#building-littlehorse)
      - [Temporary CLI Dependencies](#temporary-cli-dependencies)
  - [Understanding the Code](#understanding-the-code)
    - [Repository Structure](#repository-structure)
    - [Where the Logic Is](#where-the-logic-is)
    - [Entrypoint Classes](#entrypoint-classes)
  - [Running Examples](#running-examples)
    - [Build the Main Docker Image](#build-the-main-docker-image)
    - [Install in Docker Compose](#install-in-docker-compose)
    - [Run a Workflow](#run-a-workflow)
    - [Cleanup](#cleanup)
      - [Aside: How the Tasks Work](#aside-how-the-tasks-work)

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

### Building LittleHorse
Running the script `./build.sh` should build LittleHorse, creating an all-in-one docker image tagged `little-horse-api:latest`.

To simply build the Java binary (but not the docker image), run `gradle build` in the root of the repository. This will allow you to run an arbitrary class using the command `java -cp "./app/bin/main:./app/build/libs/app-all.jar" little.horse.SomeClass` from the root of the repository.

#### Temporary CLI Dependencies
Currently, LittleHorse has a temporary stand-in CLI implemented in Python. We plan to build a more robust version of it (most likely in Java); however, for now you need to install python (I recommend through [anaconda](https://docs.anaconda.com/anaconda/install/index.html)).

You can create your conda environment by navigating to the root of the repo, then running:
```
-> conda env update -f environment.yml
...
-> conda activate little-horse
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
* `app/src/main/java/little/horse/lib/`: This directory contains a) Java interfaces to implement Task/Workflow Deployers and Workers, and b) example implementations of those interfaces.
    * `deployers`: Contains interfaces needed to deploy the Workflow Worker and Task Workers. The `examples` subdirectory implements sample Docker and Kubernetes deployers.
    * `worker`: Contains interfaces needed to implement a LittleHorse Task Worker, and a reference implementation for a worker that executes tasks in a Docker container (this works for Kubernetes as well).
* `app/src/main/java/little/horse/sdk`: This contains scaffolding for an SDK, but I've only put two days of work into it.

### Where the Logic Is
* Core API:
    * `BaseSchema.java` is a basic serializable class (through JSON dumping/loading) used to store all imporant data.
    * `CoreMetadata.java` is a base for special metadata, such as all objects in the `PROGRAMMING_MODEL.md`, including `WFSpec`, `TaskDef`, `ExternalEventDef`, and `WFRun`. 
    * `MetadataTopologyBuilder.java` is a generic class which initializes a Kafka Streams Topology for a specifc CoreMetadata type. It handles the CRUD operations for that type, and uses the associated CoreMetadata's implementation in order to produce side effects (eg deploying a Workflow).
    * `CoreMetadataAPI.java` is the generic class that implements CRUD for Core Metadata in the REST API.
        * *Note: it is all put together in the `LittleHorseAPI.java` class.*
    * `APIStreamsContext.java` is the high-tech class used to turn Kafka Streams into a database, allowing for querying data partitioned across several RocksDB instances.
* Workflow Scheduling Logic
    * `WFRunTopology.java` initializes the Kafka Streams Topology on the Workflow Worker process. This topology is the actual scheduler that sends task schedule requests to appropriate task queues, and updates the state of the WFRun in a Kafka Topic that is listened to by the CoreMetadata Topology on the Core LittleHorse API.
    * `WFRuntime.java` shepherds the actual scheduling. See how it gets inserted in `WFRunTopology.java`.
    * `WFRun.java` and `ThreadRun.java` have the real potatoes of the actual state transition logic for what happens when an event (i.e. Task Completed) is processed.
* Deployer Interfaces
    * `WorkflowDeployer.java` is the interface for deploying a workflow.
        * `DockerWorkflowDeployer.java` and `K8sWorkflowDeployer.java` are example implementations. Note that they both use the `WorkflowWorker.java` class, see the K8s entrypoint and the Docker command.
    * `TaskDeployer.java` is the interface for deploying tasks.
        * `DockerTaskDeployer.java` and `K8sWorkflowDeployer.java` are example implementations. Note that they both use the `WorkflowWorker.java` class, see the K8s entrypoint and the Docker command.

### Entrypoint Classes
* `LittleHorseAPI.java` is run for the core API.
* `DockerTaskWorker.java` is an example of a process that executes Tasks.
* `WorkflowWorker.java` is an example of a process that does the Workflow Scheduling.

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

Let's run the `demo` workflow. The `demo` workflow is quite simple. It consists of three Node's:

1. The `ask_for_name` Node executes the `whats-your-name` task.
2. The `wait_for_name` Node waits for the `my-name` External Event to come in, and updates the `person_name` variable to be the value provided in the External Event.
3. The `greet` Node executes the `hell-there` Task, providing `person_name` as an input variable.

Before we can run it, we gotta deploy the workflow (Make sure you've activated the `little-horse` conda environment):

```
-> python examples/apply.py examples/specs/demo/*
Successfully created TaskDef hello-there
Successfully created ExternalEventDef my-name
Successfully created TaskDef whats-your-name
Successfully created WFSpec 3aa42d2038442e4297
```

You should see the following via `docker ps`:
```
->docker ps
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS          PORTS     NAMES
5cf78303fc9e   little-horse-api:latest           "java -cp /littleHor…"   3 seconds ago    Up 2 seconds              demo-3aa42d20
242b3fbc7e5f   little-horse-api:latest           "java -cp /littleHor…"   3 seconds ago    Up 2 seconds              lh-task-whats-your-name
6bc65e081168   little-horse-api:latest           "java -cp /littleHor…"   4 seconds ago    Up 3 seconds              lh-task-hello-there
6b6cdc451942   little-horse-api:latest           "java -cp /littleHor…"   58 seconds ago   Up 56 seconds             little-horse-api
70376d2e206e   confluentinc/cp-kafka:6.2.0       "/etc/confluent/dock…"   58 seconds ago   Up 57 seconds             broker
80019d5e14b4   confluentinc/cp-zookeeper:6.2.0   "/etc/confluent/dock…"   58 seconds ago   Up 57 seconds             zookeeper

```
The `demo-3aa42d20` is the Workflow Scheduler; the `lh-task-whats-your-name` is the Task Worker for the `whats-your-name` task; and the `lh-task-hello-there` is the Task Worker for the `hello-there` task.


To run the workflow, do the following:
```
-> python examples/run_wf.py demo
9cdbadb4-6c86-454e-80c9-932e3729b3ff

```
The long guid thing is the ID of the Workflow Run you've just created. You can check its status via:

```
-> python examples/wfrun_status.py 9cdbadb4-6c86-454e-80c9-932e3729b3ff
 WFRun Status:RUNNING
 Threads:
--->>
	 Id: 0
	 Status: RUNNING
	 Tasks:
		 ask_for_name: What's your name?
	 Waiting on node wait_for_name
	 Variables:
		 my_name: null

```
The WFRun is stuck waiting on the node `wait_for_name`, which means it's waiting for an External Event. Perfect! That's what's supposed to happen. Let's send an External Event to get things moving again:

```
-> python examples/send_event.py 9cdbadb4-6c86-454e-80c9-932e3729b3ff my-name Obi-Wan
{"message":null,"status":null,"objectId":null,"result":null}

```

And let's look at the workflow status to make sure it's completed:

```
-> python examples/wfrun_status.py 9cdbadb4-6c86-454e-80c9-932e3729b3ff
 WFRun Status:COMPLETED
 Threads:
--->>
	 Id: 0
	 Status: COMPLETED
	 Tasks:
		 ask_for_name: What's your name?
		 wait_for_name: Obi-Wan
		 greet: Hello there, Obi-Wan!
	 Variables:
		 my_name: "Obi-Wan"

```
You can see that the External Event was saved to the `my_name` variable, and the Workflow Run has completed. Great!!

### Cleanup

To clean up the docker containers created via the `python examples/apply.py examples/specs/demo/*` command above, you can:

```
-> python examples/delete.py examples/specs/demo/*
```

To clean up the LittleHorse Core API and Kafka containers, you can:
```
-> ./docker/cleanup.sh
```

#### Aside: How the Tasks Work
Look at the `*_task.json` files in `examples/specs/demo/` to see how the `TaskDef`'s are formed. Essentially, they use the `BashExecutor` class as the executor for the `DockerTaskWorker`, and you can see the bash command in the doubly-encoded Json string if you look close enough.

Next, peek at the `Dockerfile` in the root of the repo. There you can see that for debugging we copy all of the `examples/tasks` into the `little-horse-api` docker image so that we can run them with the `BashExecutor`. This is going to be removed by the time we have our production release, but for now, it is convenient to be able to easily put a python file in `/examples/tasks` and use it for developing workflows.
