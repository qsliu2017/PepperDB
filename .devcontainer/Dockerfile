FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Install dependencies for building PostgreSQL source (for reference reading)
RUN apt-get update && apt-get install -y \
    build-essential \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    pkg-config \
    libssl-dev \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Clone PostgreSQL 18 source code for reference
RUN git clone --depth 1 --branch master https://github.com/postgres/postgres.git /opt/postgres-src \
    && chown -R vscode:vscode /opt/postgres-src
