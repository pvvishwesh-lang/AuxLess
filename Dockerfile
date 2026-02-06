FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install flask apache-beam[gcp] google-cloud-firestore requests

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "backend/pipelines/api/server.py"]
