FROM continuumio/miniconda3 AS base

COPY . .
RUN conda env update -f environment.yml
ENTRYPOINT ["./entrypoint.sh"]