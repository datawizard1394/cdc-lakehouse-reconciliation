FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --system cdc \
    && useradd --system --gid cdc --create-home cdc

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

USER cdc
ENTRYPOINT ["cdc-reconcile"]
CMD ["--help"]

