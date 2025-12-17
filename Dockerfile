FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080

CMD gunicorn app:app --bind 0.0.0.0:$PORT --timeout 0 --worker-class gevent --workers 1
