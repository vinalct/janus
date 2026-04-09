FROM python:3.13.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends make openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY conf ./conf
COPY docs ./docs
COPY tests ./tests
COPY data ./data

RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt \
    && python -m pip install --no-deps .

CMD ["python", "-m", "janus.main", "--environment", "local", "--with-spark"]
