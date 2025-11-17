FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

RUN uv pip install --system "numpy>=1.26.0" "pillow>=11.0.0"
RUN uv pip install --system git+https://github.com/WHOIGit/ifcb-features.git
RUN uv pip install --system 'git+https://github.com/WHOIGit/ifcb-features-microservice-poc.git#egg=amplify-stateful&subdirectory=.'

COPY . ./app

EXPOSE 8000

CMD ["uvicorn", "examples.ifcb_features_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
