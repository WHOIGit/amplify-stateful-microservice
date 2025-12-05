FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

# Install minimal dependencies
RUN uv pip install --system "pillow>=11.0.0"

# Copy and install the stateful_microservice library
COPY stateful_microservice /app/stateful_microservice
COPY pyproject.toml README.md /app/
RUN uv pip install --system -e .

# Copy this service
COPY examples/simple_websocket_test /app/service

WORKDIR /app/service

EXPOSE 8032

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8032"]
