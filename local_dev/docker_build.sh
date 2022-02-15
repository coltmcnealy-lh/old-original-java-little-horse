#!/bin/bash

set -ex

gradle fatJar

docker build -f Dockerfile -t little-horse-api .
