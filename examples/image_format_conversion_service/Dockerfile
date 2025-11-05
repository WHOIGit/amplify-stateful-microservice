FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml ./
COPY README.md ./
COPY ifcb_microservice ./ifcb_microservice
COPY client ./client
COPY examples ./examples

RUN uv pip install --system .

EXPOSE 8010

CMD ["uvicorn", "examples.image_format_conversion_service.main:app", "--host", "0.0.0.0", "--port", "8010"]
