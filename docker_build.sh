#!/bin/bash

set -ex

gradle fatJar

docker build -f Dockerfile -t little-horse-daemon .
docker build -f Dockerfile -t little-horse-api .
docker build -f Dockerfile -t little-horse-collector .
