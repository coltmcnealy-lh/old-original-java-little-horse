FROM openjdk:17-slim AS base

RUN apt update && \
    apt install -y curl wget dnsutils python3

COPY app/build/libs/app-all.jar /littleHorse.jar

COPY examples /examples
COPY starwars_example/tasks /starwarstasks

CMD ["java", "little.horse.App"]