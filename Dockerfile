FROM python:3.11-slim

WORKDIR /app

RUN pip install psycopg2-binary faker

COPY db-generate-data.py .

CMD ["python", "db-generate-data.py"]