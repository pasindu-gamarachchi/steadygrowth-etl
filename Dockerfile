FROM python:3.9-slim

WORKDIR /etl

COPY requirements.txt /etl
COPY etl etl

RUN pip3 install -r requirements.txt
CMD ["python3", "./etl/main.py"]