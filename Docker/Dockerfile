FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip
RUN pip install -r Docker/requirements.txt

EXPOSE 8080

CMD ["python", "backend/pipelines/api/server.py"]
