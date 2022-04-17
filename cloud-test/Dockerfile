FROM python:3.10-slim AS base

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y libpq-dev python-dev openjdk-17-jre-headless gcc && \
    apt-get clean;

COPY cloud-test/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

ENV PYTHONPATH=/cloud-test/

COPY app/build/libs/app-all.jar /littleHorse.jar

COPY cloud-test /cloud-test
