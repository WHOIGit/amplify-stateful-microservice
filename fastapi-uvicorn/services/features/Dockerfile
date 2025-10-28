FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
ENV PATH="/root/.local/bin:$PATH"

# Install base framework
COPY base /tmp/base
RUN cd /tmp/base && uv pip install --system .

# Install ifcb-features and its dependencies
RUN git clone https://github.com/WHOIGit/ifcb-features.git /tmp/ifcb-features && \
    cd /tmp/ifcb-features && \
    uv pip install --system . && \
    rm -rf /tmp/ifcb-features

# Install service-specific dependencies
COPY services/features/pyproject.toml ./
RUN uv pip install --system .

# Copy service code
COPY services/features/app /app/app

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
