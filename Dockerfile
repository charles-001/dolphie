FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./

# Extract version from pyproject.toml
RUN DOLPHIE_VERSION=$(sed -n 's/^version = "\([^"]*\)"/\1/p' pyproject.toml) \
    && pip3 install --no-cache-dir dolphie=="$DOLPHIE_VERSION"
