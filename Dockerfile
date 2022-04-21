# This Dockerfile has all of the following:
# CORE RUNTIME:
# * API Server
# * Workflow and Task Deployer Implementations (everything in the app/src/main/...)
# * Workflow Scheduler Code
# * Workflow Task Worker Implementations (everything in app/src/main/...)
# 
# SDK:
# * The Python SDK code
# * A properly-configured base python environment with all of the SDK's depencies
#   installed.

FROM python:3.10-slim AS base

# Install:
# * OpenJDK 17 (Java) for the core runtime code
# * Various dependencies for Pip stuff
RUN apt update && \
    apt install -y curl wget dnsutils libpq-dev python-dev openjdk-17-jre-headless gcc && \
    apt-get clean;


# Install Kubectl (used by the Kubernetes deployer)
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl


# Install the python dependencies
COPY lh-sdk-python/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt


# Copy the Java jars
COPY app/build/libs/app-all.jar /littleHorse.jar


# Copy in the SDK and set the pythonpath so that it's usable.
COPY lh-sdk-python /lh-sdk-python
ENV PYTHONPATH /lh-sdk-python/


# Bring some dummy tasks (used for demo purposes only)
COPY examples/tasks /starwarstasks

# This command just prints a message and exits. In other words, it should
# be overriden by whatever is in charge of running the app.
CMD ["java", "-cp", "/littleHorse.jar", "little.horse.App"]
