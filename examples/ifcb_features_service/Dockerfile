FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

# Install dependencies
RUN uv pip install --system "numpy>=1.26.0" "pillow>=11.0.0"
RUN uv pip install --system git+https://github.com/WHOIGit/ifcb-features.git

# Copy and install the stateful_microservice library from this repo
COPY stateful_microservice /app/stateful_microservice
COPY pyproject.toml README.md /app/
RUN uv pip install --system -e .

# Copy only this specific service
COPY examples/ifcb_features_service /app/service

WORKDIR /app/service

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
