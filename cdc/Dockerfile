FROM python:3.14.2-slim-bookworm

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y libaio1 wget unzip build-essential && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2326000/instantclient-basiclite-linux.x64-23.26.0.0.0.zip && \
    unzip instantclient-basiclite-linux.x64-23.26.0.0.0.zip && \
    ls -la && \
    find /app -name "libclntsh*" && \
    rm instantclient-basiclite-linux.x64-23.26.0.0.0.zip && \
    echo /app/instantclient_23_26 > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig && \
    ldconfig -p | grep libclntsh

ENV LD_LIBRARY_PATH=/app/instantclient_23_26:$LD_LIBRARY_PATH

# Install Python deps
RUN pip install cx_Oracle kafka-python

# Remove build tools
RUN apt-get remove -y build-essential && apt-get autoremove -y

COPY cdc_reader.py .

CMD ["python", "cdc_reader.py"]