FROM jupyter/base-notebook:python-3.10

USER root  

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY requirements.txt /tmp/requirements.txt

RUN uv pip install --system --no-cache -r /tmp/requirements.txt

USER ${NB_UID} 