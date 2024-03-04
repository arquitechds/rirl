ARG BASE_CONTAINER=jupyter/minimal-notebook
FROM python:3.9
FROM $BASE_CONTAINER

USER root
# Installing geopandas and all it's dependencies
RUN conda install -c conda-forge movingpandas && \
    conda clean --all -f -y && \
    rm -rf /home/$NB_USER/.cache/yarn 

RUN conda install -c conda-forge shapely 