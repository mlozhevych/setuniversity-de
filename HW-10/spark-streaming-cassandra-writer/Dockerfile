#FROM python:3.11-slim
FROM apache/spark:3.5.6-java17-python3

USER root

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Install Java for PySpark
#RUN apt-get update && apt-get install -y \
#    openjdk-17-jdk-headless \
#    curl \
#    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#ENV PATH=$PATH:$JAVA_HOME/bin

# Copy poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Configure poetry to not use virtual environments
# RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-ansi --no-root

# Copy source code
COPY src/ .

# Create checkpoint directory
RUN mkdir -p /opt/spark/work-dir/checkpoints/writer
RUN chmod 777 /opt/spark/work-dir/checkpoints/writer

ENV PYTHONUNBUFFERED=1

# Run the writer
CMD ["poetry", "run", "python", "-m", "spark_cassandra_writer.writer"]