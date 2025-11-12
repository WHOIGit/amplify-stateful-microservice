FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

COPY pyproject.toml ./
COPY README.md ./
COPY stateful_microservice ./stateful_microservice
COPY client ./client
COPY examples ./examples

RUN uv pip install --system .
RUN uv pip install --system git+https://github.com/WHOIGit/ifcb-features.git

EXPOSE 8000

CMD ["uvicorn", "examples.ifcb_features_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
