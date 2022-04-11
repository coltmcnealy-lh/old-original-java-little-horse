FROM continuumio/miniconda3 AS base

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean;

COPY cloud-test/environment.yml /environment.yml
RUN conda env update -f /environment.yml

COPY app/build/libs/app-all.jar /littleHorse.jar

COPY cloud-test /cloud-test
