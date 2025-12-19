FROM python:3.14.2-slim-bookworm

WORKDIR /app

RUN pip install oracledb

COPY data_generator.py .

CMD ["python", "data_generator.py"]
