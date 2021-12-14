FROM openjdk:18-slim AS base
RUN apt update && \
    apt install -y curl python3

RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl


COPY app/build/libs/app-all.jar /littleHorse.jar

COPY examples /examples

ENTRYPOINT ["java", "-jar", "/littleHorse.jar"]