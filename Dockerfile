FROM python:3.12-slim

# Install gawk for parsing pyproject.toml
RUN apt-get update && apt-get install -y --no-install-recommends gawk

# Extract the version from pyproject.toml and store it in a variable
ARG DOLPHIE_VERSION
WORKDIR /app
COPY pyproject.toml ./

# Extract version from pyproject.toml
RUN DOLPHIE_VERSION=$(gawk -F'"' '/^version =/ {print $2}' pyproject.toml) \
    && pip3 install --no-cache-dir dolphie==$DOLPHIE_VERSION