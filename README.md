# LittleHorse Runtime

This repository contains code for the LittleHorse Runtime.

## Development
This section is a quickstart on running and developing LittleHorse locally.

### Dependencies
The following software is needed to develop LittleHorse:

* `openjdk`, tested with version `17.0.2 2022-01-18`.
* `gradle` version `7.3` or later.
* `docker` CLI and engine, tested with version `20.10.14`.
* Optional: `kubectl` version 16 or later.
* Optional: `kind` version `v0.11.1` or later.

### Building LittleHorse
Running the script `./build.sh` should build LittleHorse, creating an all-in-one docker image tagged `little-horse-api:latest`.

To simply build the Java binary (but not the docker image), run `gradle build` in the root of the repository. This will allow you to run an arbitrary class using the command `java -cp "./app/bin/main:./app/build/libs/app-all.jar" little.horse.SomeClass` from the root of the repository.

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

### Important Classes