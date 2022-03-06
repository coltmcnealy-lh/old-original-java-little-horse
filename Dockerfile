RUN apt update && \
    apt install -y curl wget dnsutils python3

COPY app/build/libs/app-all.jar /littleHorse.jar

COPY examples /examples

ENTRYPOINT ["java", "-jar", "/littleHorse.jar"]