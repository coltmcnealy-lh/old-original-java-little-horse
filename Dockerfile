FROM openjdk:18-slim AS base

COPY app/build/libs/app-all.jar /littleHorse.jar

ENTRYPOINT ["java", "-jar", "/littleHorse.jar"]