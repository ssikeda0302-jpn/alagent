FROM python:3.11-slim

WORKDIR /app

RUN pip install nanobot

COPY config.json /app/config.json

RUN mkdir -p /app/workspace

COPY workspace/ /app/workspace/

CMD ["nanobot", "gateway", "--config", "/app/config.json"]
