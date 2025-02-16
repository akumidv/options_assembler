# syntax=docker/dockerfile:1
# `python-base` sets up all our shared environment variables
FROM python:3.13.1-slim AS python_base
# python
ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # paths
    # this is where our requirements + virtual environment will live
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$VENV_PATH/bin:$PATH"

# `builder-base` stage is used to build deps + create our virtual environment
FROM python_base AS builder_base

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry https://python-poetry.org/docs/configuration/#using-environment-variables
    POETRY_VERSION=2.0.1 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    curl \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -


FROM builder_base AS packages
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./

RUN poetry install --only=main --no-root

FROM python_base AS etl_code
COPY --from=packages $PYSETUP_PATH $PYSETUP_PATH

WORKDIR /app

ENV PYTHONPATH="/app"\
    # App environment
    DATA_PATH="/app/data" \
    ETL_TIMEFRAME="1h"

#ENV TG_BOT_TOKEN=""\
#   TG_CHAT=""

COPY ./src /app/
COPY ./demo/etl_example/ /app/

FROM etl_code AS etl_deribit

ENTRYPOINT ["python", "/app/deribit/deribit_etl_run.py"]
#ENTRYPOINT ["poetry", "run", "python", "./etl_example/deribit_etl_run.py"]
# Do not forget attach /app/data in workspace folder to external folder or set DATA_PATH
