FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Configure poetry to not use virtual environments
# RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-ansi --no-root

# Copy source code
COPY src/ .

ENV PYTHONUNBUFFERED=1

# Run the generator
CMD ["poetry", "run", "python", "-m", "stream_generator.generator"]
