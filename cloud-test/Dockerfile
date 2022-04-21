# This Dockerfile adds some small dependencies for the LittleHorse test harness.
FROM little-horse-api:latest


# Copy in the test harness code
COPY cloud-test /cloud-test
ENV PYTHONPATH ${PYTHONPATH}:/cloud-test/
