FROM python:3.9-slim

WORKDIR /etl

COPY requirements.txt /etl
RUN pip3 install -r requirements.txt

COPY etl etl

CMD ["python3", "./etl/main.py"]