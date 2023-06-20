FROM python:3.9-alpine

RUN apk add --no-cache python3-dev gcc musl-dev libffi-dev libpq-dev openssl-dev make && \
    apk add --no-cache redis

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["sh", "-c", "redis-server & uvicorn app:app --reload --host 0.0.0.0"]


