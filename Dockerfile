FROM openjdk:17-slim AS base

RUN apt update && \
    apt install -y curl wget dnsutils python3

RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl

COPY app/build/libs/app-all.jar /littleHorse.jar

COPY starwars_docker/tasks /starwarstasks

CMD ["java", "-cp", "/littleHorse.jar", "little.horse.App"]
