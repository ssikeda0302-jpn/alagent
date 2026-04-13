FROM python:3.11-slim

WORKDIR /app

RUN pip install discord.py anthropic

COPY bot.py /app/bot.py
COPY workspace/ /app/workspace/

CMD ["python", "bot.py"]
