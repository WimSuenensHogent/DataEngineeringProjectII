FROM python:3.8-slim-buster

# install system dependencies
RUN apt-get update \
  && apt-get -y install gcc \
  && apt-get -y install g++ \
  && apt-get -y install unixodbc unixodbc-dev \
  && apt-get clean

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Initializing new working directory
WORKDIR /code

# Transferring the code and essential data
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini
COPY settings.py ./settings.py
COPY settings.yaml ./settings.yaml
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY main.py ./main.py

RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system