# Use the official Airflow image as the base image
FROM apache/airflow:latest-python3.9

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Verify installation (optional)
RUN java -version
RUN ls -la

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir --verbose -r /requirements.txt

