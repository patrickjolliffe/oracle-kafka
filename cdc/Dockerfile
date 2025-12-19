FROM python:3.14.2-slim-bookworm

WORKDIR /app

RUN pip install oracledb kafka-python

COPY cdc_reader.py .

CMD ["python", "cdc_reader.py"]