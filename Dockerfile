# One image for all Python services: Dagster (webserver + daemon), the Streamlit
# dashboard and the KVK agent. The data is not in the image: lakehouse/ and inbox/ are
# mounted from the project folder (see docker-compose.yml).
FROM python:3.12-slim

# The code version recorded in lakehouse/_run_log (there is no git inside the image):
#   docker compose build --build-arg CODE_VERSION=$(git rev-parse --short HEAD)
ARG CODE_VERSION=unknown
ENV CODE_VERSION=${CODE_VERSION} \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp

WORKDIR /app

# Dependencies first, so a code change doesn't reinstall them (Docker layer cache).
# dagster-postgres: in Docker, Dagster keeps its run history in Postgres. It works with
# the psycopg2 driver; SQLAlchemy 2.1 switched its default Postgres driver to psycopg 3,
# with which Dagster's queued runs fail (SQL syntax error), so SQLAlchemy stays on 2.0.
COPY requirements.txt .
RUN pip install -r requirements.txt "dagster-postgres>=0.25" "psycopg2-binary>=2.9" \
    "sqlalchemy>=2.0,<2.1"

COPY . .

# Containers run as the host user (see docker-compose.yml), so make the app readable
# for anyone; nothing in /app is written at runtime except the mounted folders.
RUN chmod -R a+rX /app

# Dagster's home: its settings, plus run logs that the webserver and the daemon share
# through the dagster_home volume. A new volume copies this folder, permissions
# included, so the non-root user can write there.
ENV DAGSTER_HOME=/opt/dagster_home
RUN mkdir -p /opt/dagster_home \
    && cp orchestration/docker/dagster.yaml /opt/dagster_home/ \
    && chmod -R a+rwX /opt/dagster_home

EXPOSE 3000 8501
